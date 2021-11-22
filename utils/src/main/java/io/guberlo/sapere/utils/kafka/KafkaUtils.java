package io.guberlo.sapere.utils.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class KafkaUtils {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    public KafkaUtils() {
    }

    /**
     * Read data from kafka
     *
     * @param topic specify from which topic to read
     * @param brokers specify the kafka brokers
     * @return dataset of data read from the topic
     */
    public Dataset<Row> readKafkaStream(String topic, String brokers) {
        SparkSession session = SparkSession.builder()
                .appName("Kafka Utils")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        return session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("startingOffsets", "latest")
                .option("subscribe", topic)
                .load();
    }

    /**
     * Send the request to the 'processRequest' topic.
     * Consumers will then pick up requests from this topic.
     *
     * @param dataset: DataFrame must have columns | id | |text|.
     * @param topic: str Kafka topic where to send data.
     * @param broker: str Kafka brokers.
     * @throws TimeoutException
     */
    public void writeStreamKafka(Dataset<Row> dataset, String topic, String broker) throws TimeoutException {
        dataset.select(functions.to_json(functions.struct("id", "text")).alias("value"))
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("topic", topic)
                .option("checkpointLocation", "/tmp/kafka/checkpoint")
                .start();
    }

}
