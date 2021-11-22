package io.guberlo.sapere.consumer.product;

import com.neodatagroup.jero.spark.JeroTransformer;
import io.guberlo.sapere.consumer.model.Consumer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.util.concurrent.TimeoutException;

public class JeroConsumer extends Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(JeroConsumer.class);
    private static final String jeroPath = "hdfs://10.0.100.80:9000/opt/app/parquet/";
    private static final transient JeroTransformer transformer = JeroTransformer.read().load(jeroPath);

    public JeroConsumer(String configPath, String label) {
        super(configPath, label);
    }

    @Override
    public Row predict(Row row) {

        return null;
    }

    @Override
    public void elaborate(Dataset<Row> dataset, Long datasetId) {
        SparkSession session = SparkSession.builder().getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String outputTopic = config.getKafkaConfig().getProcessTopic().get(1);
        String broker = config.getKafkaConfig().getBrokers();

        Dataset<Row> passages = dataset.select("id", "text")
                .withColumnRenamed("id", "passage_id")
                .withColumnRenamed("text", "passage");

        Dataset<Row> annotations = transformer.transform(passages);

        if (annotations.isEmpty()) {
            LOG.warn("********* RDD EMPTY *********");
            return;
        }

        // Flatten jero predictions into one string and insert relative text since it was removed on the transformer elaboration
        Dataset<Row> predictions = annotations.select("*")
                .groupBy("passage_id")
                .agg(functions.collect_list(functions.array("start", "end", "mention", "entity_id", "entity_name", "entity_type", "rho", "confidence")).as("prediction"))
                .join(passages, "passage_id");

        // Add type Column which is needed in order to save it to ES, rename columns and send to Kafka
        predictions.select(functions.col("passage_id"), functions.col("passage"), functions.col("prediction"), functions.lit("Jero Wikifier").as("type"))
                .withColumnRenamed("passage_id", "id")
                .withColumnRenamed("passage", "text")
                .select(functions.to_json(functions.struct("id", "text", "prediction", "type")).alias("value"))
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("topic", outputTopic)
                .save();
    }
}
