package io.guberlo.sapere.model;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public abstract class Consumer implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private transient Config config;
    private String configPath;
    protected String label;

    private StructType inputSchema = new StructType(new StructField[] {
            new StructField("id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("text", DataTypes.StringType, false, Metadata.empty())
    });

    private StructType outputSchema = new StructType(new StructField[] {
            new StructField("id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("type", DataTypes.StringType, false, Metadata.empty()),
            new StructField("text", DataTypes.StringType, false, Metadata.empty()),
            new StructField("prediction", DataTypes.StringType, false, Metadata.empty())
    });

    public Consumer() {
    }

    public Consumer(String configPath, String label) {
        LOG.debug("New consumer created, configPath: {}, label: {}", configPath, label);
        this.configPath = configPath;
        this.config = new Config().getYaml(configPath);
        this.label = label;
    }

    /**
     * Read data stream from kafka into a DataFrame.
     *
     * @return Request dataset
     */
    public Dataset<Row> readStreamKafka() {
        SparkSession session = SparkSession.builder()
                .appName("Consumer")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        return session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", this.config.getKafkaConfig().getBrokers())
                .option("startingOffsets", "earliest")
                .option("subscribe", this.config.getKafkaConfig().getGeneralTopic())
                .load();
    }

    public void writeToConsole(Dataset<Row> dataset) throws StreamingQueryException, TimeoutException {
        dataset.selectExpr("CAST(value AS STRING)")
                .writeStream()
                .format("console")
                .start()
                .awaitTermination();
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

    /**
     * Apply the predict algorithm to each element of the Stream batch.
     * Send results to the 'processResponse' Kafka topic as output.
     *
     * @param dataset Mini batch dataframe with id | text as columns.
     * @param datasetId The id of the mini batch dataframe.
     */
    public void elaborate(Dataset<Row> dataset, Long datasetId) {
        SparkSession session = SparkSession.builder().getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String outputTopic = config.getKafkaConfig().getProcessTopic().get(1);
        String broker = config.getKafkaConfig().getBrokers();

        JavaRDD<Row> rdd = dataset.javaRDD().map(row -> predict(row));

        if(rdd.isEmpty()) {
            System.out.println("********* RDD EMPTY *********");
            return;
        }

        Dataset<Row> output = session.createDataFrame(rdd, outputSchema);
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
                .select(functions.from_json(new Column("value"), inputSchema).alias("data"))
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
        Dataset<Row> dataset = readStreamKafka();
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

    public StructType getInputSchema() {
        return inputSchema;
    }

    public void setInputSchema(StructType inputSchema) {
        this.inputSchema = inputSchema;
    }

    public StructType getOutputSchema() {
        return outputSchema;
    }

    public void setOutputSchema(StructType outputSchema) {
        this.outputSchema = outputSchema;
    }

}
