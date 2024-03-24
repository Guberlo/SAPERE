from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as tp
from pyspark.sql.functions import from_json, to_json, struct

from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

class Consumer:

    def __init__(self) -> None:
        self.yamlPath = "/opt/spark/config/config.yaml"
        self.cfg = self.getYaml(self.yamlPath)

        self.brokers = self.cfg['kafka']['brokers']
        self.topic = self.cfg['kafka']['generalTopic']
        self.processTopics = self.cfg['kafka']['processTopics']

        self.elasticHost = self.cfg['elastic']['host']
        self.index = self.cfg['elastic']['index']

        self.inputSchema = tp.StructType([
            tp.StructField(name='id', dataType=tp.StringType(), nullable=False),
            tp.StructField(name='text', dataType=tp.StringType(), nullable=False)
        ])
        self.outputSchema = tp.StructType([
            tp.StructField(name='id', dataType=tp.StringType(), nullable=False),
            tp.StructField(name='type', dataType=tp.StringType(), nullable=False),
            tp.StructField(name='text', dataType=tp.StringType(), nullable=False),
            tp.StructField(name='prediction', dataType=tp.StringType(), nullable=False)
        ])

    def getYaml(self, path: str) -> dict:
        """
            Load the yaml configuration file into a dictionary.
            
            @param path: path to yaml file
        """
        with open(path) as yamlfile:
            cfg = load(yamlfile, Loader=Loader)

        print(type(cfg))
        return cfg

    def readStreamKafka(self) -> DataFrame:
        """
            Read data stream from kafka into a DataFrame.
        """
        session = SparkSession.builder.getOrCreate()
        session.sparkContext.setLogLevel("WARN")

        return session.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.brokers) \
            .option("startingOffsets", "latest") \
            .option("subscribe", self.topic) \
            .load()

    def writeStreamKafka(self, words: DataFrame, topic: str, brokers: str) -> None:
        """
            Send the request to the 'processRequest' topic.
            Consumers will then pick up requests from this topic.

            @param words: DataFrame must have columns | id | |text|.
            @param topic: str Kafka topic where to send data.
            @param brokers: str Kafka brokers.
        """
        words.select(to_json(struct("id", "text")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("topic", topic) \
            .option("checkpointLocation", "/tmp/kafka/checkpoint") \
            .start()

    def elaborate(self, batch_df: DataFrame, batch_id: int) -> None:
        """
            Apply the predict algorithm to each element of the Stream batch.
            Send results to the 'processResponse' Kafka topic as output.

            @param batch_df: Mini batch dataframe with id | text as columns.
            @param batch_id: The id of the mini batch dataframe.
        """
        session = SparkSession.builder.getOrCreate()
        session.sparkContext.setLogLevel("WARN")

        rdd = batch_df.rdd \
                .map(self.predict)

        if rdd.isEmpty():
            print("********* RDD EMPTY *********")
            return

        # TODO: external function?
        df = session.createDataFrame(rdd, self.outputSchema)
        df.select(to_json(struct("id", "type", "text", "prediction")).alias("value")) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.brokers) \
            .option("topic", self.processTopics[1]) \
            .save()

    def foreachPredict(self, stream: DataFrame) -> None:
        """
            Process the passed DataFrame, which should have
            only key | value columns, applying the prediction
            and send it to the 'processResponse' Kafka topic. 
        """
        stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", self.inputSchema).alias("data")) \
            .select("data.*") \
            .writeStream \
            .foreachBatch(self.elaborate) \
            .start() \
            .awaitTermination()

    def toRow(self, data: Row, prediction: str) -> Row:
        """
            Converts the structure of the passed Row, adding the
            'prediction' field to it, so that it can be sent to Kafka.
            Since Spark Rows are immutable, we need to create a new Row
            With the new structure.

            @param data: Row in which we need to add the new label
            @param prediction: str the consumer prediction
        """
        return Row(id = data.id, type = self.label, text = data.text, prediction = prediction)

    def predict(data: Row) -> Row:
        """
            Apply prediction algorithm.
            This is consumer-specific and needs to be
            Implemented in each consumer.

            @param data: Row with id | text columns
            @return Row with id | type | text | prediction columns.
        """
        pass

    def start(self) -> None:
        """
            Start the whole process of reading, predicting and writing to kafka.
        """
        stream = self.readStreamKafka()
        self.foreachPredict(stream)