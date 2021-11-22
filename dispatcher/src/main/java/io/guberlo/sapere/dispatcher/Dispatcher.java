package io.guberlo.sapere.dispatcher;

import io.guberlo.sapere.utils.config.Config;
import io.guberlo.sapere.utils.elastic.ElasticUtils;
import io.guberlo.sapere.utils.kafka.KafkaUtils;
import io.guberlo.sapere.utils.spark.SparkUtils;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;


public class Dispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);
    private final KafkaUtils kafkaUtils;
    private final SparkUtils sparkUtils;
    private transient final ElasticUtils elasticUtils;
    private transient final Config config;
    private final String broker;
    private final String topic;
    private final List<String> processTopic;

    public Dispatcher(String lang, String configPath) {
        config = new Config().getYaml(configPath);
        kafkaUtils = new KafkaUtils();
        sparkUtils = new SparkUtils(lang);
        elasticUtils = new ElasticUtils(config.getElasticConfig().getIndex(), config.getElasticConfig().getHost());
        broker = config.getKafkaConfig().getBrokers();
        topic = config.getKafkaConfig().getGeneralTopic();
        processTopic = config.getKafkaConfig().getProcessTopic();
    }

    public Dataset<Row> preProcess(Dataset<Row> dataset) {

        // 1. Convert from Kafka schema to spark and remove NA records
        Dataset<Row> converted = fromKafkaToSpark(dataset);
        if (converted == null) {
            LOG.info("Dataset is empty");
            return null;
        }

        // 2. If specified in the request, tokenize and remove stop words
        // TODO: Define how to establish if the request has those settings
        if (1 == 0)
            return sparkUtils.removeStopWords(sparkUtils.tokenize(converted));

        return converted;
    }

    private void func(Dataset<Row> batch, Long batchId) {
        batch.foreachPartition(elasticUtils::updateOnES);
    }

    /**
     * IMPORTANT: this method is used only when the document is already
     * Indexed on elastic search
     *
     * Parse the received DataFrame into the appropriate schema
     * And apply a function to save data into Elastic Search with
     * The corrisponding predictions.
     *
     * @param output dataset off predictions to send to elastic
     */
    private void updateOnElastic(Dataset<Row> output) throws TimeoutException, StreamingQueryException {
        output.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), sparkUtils.getOutputSchema()).alias("data"))
                .select("data.*")
                .writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) this::func)
                .start()
                .awaitTermination();
    }

    public void start() throws TimeoutException, StreamingQueryException {
        Dataset<Row> stream = kafkaUtils.readKafkaStream(topic, broker);
        stream = preProcess(stream);
        kafkaUtils.writeStreamKafka(stream, processTopic.get(0), broker);

        Dataset<Row> output = kafkaUtils.readKafkaStream(processTopic.get(1), broker);
        updateOnElastic(output);
    }

    private Dataset<Row> fromKafkaToSpark(Dataset<Row> dataset) {
        return dataset.selectExpr("CAST(value AS STRING)")
                .alias("json")
                .select(functions.from_json(functions.col("value"), sparkUtils.getInputSchema()).alias("data"))
                .select("data.*")
                .na().drop();

    }

}
