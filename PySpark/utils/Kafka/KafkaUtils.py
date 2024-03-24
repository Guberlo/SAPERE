from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_json, struct

def readStreamKafka(topic: str, brokers: str) -> DataFrame:
    """
        Read data from kafka

        @param topic is the topic to read from.
        @param brokers: str Kafka brokers.
    """
    session = SparkSession.builder.getOrCreate()

    return session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic) \
        .load()

def writeStreamKafka(words: DataFrame, topic: str, brokers: str) -> None:
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