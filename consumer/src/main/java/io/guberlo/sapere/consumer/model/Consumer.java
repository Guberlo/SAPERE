package io.guberlo.sapere.consumer.model;

import io.guberlo.sapere.utils.kafka.KafkaUtils;
import io.guberlo.sapere.utils.config.Config;
import io.guberlo.sapere.utils.spark.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public abstract class Consumer implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    protected transient Config config;
    protected transient KafkaUtils kafkaUtils;
    protected transient SparkUtils sparkUtils;
    private String configPath;
    protected String label;


    public Consumer() {
    }

    public Consumer(String configPath, String label) {
        LOG.debug("New consumer created, configPath: {}, label: {}", configPath, label);
        this.configPath = configPath;
        this.config = new Config().getYaml(configPath);
        this.label = label;
        kafkaUtils = new KafkaUtils();
        sparkUtils = new SparkUtils("english");
    }

    /**
     * Apply the predict algorithm to each element of the Stream batch.
     * Send results to the 'processResponse' Kafka topic as output.
     *
     * @param dataset Mini batch dataframe with id | text as columns.
     * @param datasetId The id of the mini batch dataframe.
     */
    public void elaborate(Dataset<Row> dataset, Long datasetId) throws StreamingQueryException, TimeoutException {
        SparkSession session = SparkSession.builder().getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String outputTopic = config.getKafkaConfig().getProcessTopic().get(1);
        String broker = config.getKafkaConfig().getBrokers();

        JavaRDD<Row> rdd = dataset.javaRDD().map(this::predict);

        if(rdd.isEmpty()) {
            LOG.info("********* RDD EMPTY *********");
            return;
        }

        Dataset<Row> output = session.createDataFrame(rdd, sparkUtils.getOutputSchema());
        output.select(functions.to_json(functions.struct("id", "type", "text", "prediction")).alias("value"))
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("topic", outputTopic)
                .save();

    }

    /**
     * Process the passed DataFrame, which should have
     * only key | value columns, applying the prediction
     * and send it to the 'processResponse' Kafka topic.
     * @param dataset Dataset with Kafka key | value
     * @throws TimeoutException
     * @throws StreamingQueryException
     */
    public void foreachPredict(Dataset<Row> dataset) throws TimeoutException, StreamingQueryException {
        dataset.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(new Column("value"), sparkUtils.getInputSchema()).alias("data"))
                .select("data.*")
                .writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) this::elaborate)
                .start()
                .awaitTermination();
    }

    /**
     * Apply prediction algorithm.
     * This is consumer-specific and needs to be
     * Implemented in each consumer.
     *
     * @param row Row with id | text columns
     * @return Row with id | type | text | prediction columns.
     */
    abstract public Row predict(Row row);

    /**
     * Converts the structure of the passed Row, adding the
     * 'prediction' field to it, so that it can be sent to Kafka.
     * Since Spark Rows are immutable, we need to create a new Row
     * With the new structure.
     *
     * @param data Row in which we need to add the new label
     * @param prediction the consumer prediction
     * @return new Row with new structure
     */
    public Row toRow(Row data, String prediction) {
        return RowFactory.create(data.getAs("id"), label, data.getAs("text"), prediction);
    }

    /**
     * Start the whole process of reading, predicting and writing to kafka.
     * @throws TimeoutException
     * @throws StreamingQueryException
     */
    public void start() throws TimeoutException, StreamingQueryException {
        String topic = config.getKafkaConfig().getGeneralTopic();
        String brokers = config.getKafkaConfig().getBrokers();

        Dataset<Row> dataset = kafkaUtils.readKafkaStream(topic, brokers);
        foreachPredict(dataset);
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

}
