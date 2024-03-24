from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameWriter

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, struct, to_json

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# imported using --py-files
from KafkaUtils import readStreamKafka, writeStreamKafka
from ConfigUtils import getYaml
from SparkUtils import inputSchema, outputSchema, writeToConsole

cfg = getYaml("/opt/spark/config/config.yaml")

processTopics = cfg['kafka']['processTopics']
brokers = cfg['kafka']['brokers']
label = "sentiment"

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

context = SparkContext(appName="Sentiment Consumer")
context.setLogLevel("WARN")
session = SparkSession(context)



def predict(data: Row) -> Row:
    """
        Apply Sentiment Analysis using a lexicon approach to the sentence.
    """
    return is_positive(data)

def is_positive(data: Row) -> Row:
    """
        Return the polarity of the sentiment, not taking into account neutral.
    """
    if sia.polarity_scores(data.text)['compound'] > 0:
        return Row(id = data.id, type = label, text= data.text, sentiment = "positive")
    
    return Row(id = data.id, type = label, text= data.text, sentiment = "negative")

def elaborate(batch_df: DataFrame, batch_id: int) -> None:
    """
        Apply Sentiment Analysis using a lexicon approach to the mini batch.
        Send the result to the 'processResponse' Kafka topic as output.
    """
    rdd = batch_df.rdd \
            .map(predict)

    print(rdd.collect())

    if rdd.isEmpty():
        print("********* RDD EMPTY *********")
        return

    df = session.createDataFrame(rdd, outputSchema)
    df.select(to_json(struct("id", "type", "text", "prediction")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", processTopics[1]) \
        .save()

# TODO: common function
def foreachPredict(stream: DataFrame) -> None:
    """
        Process the passed DataFrame, which should have
        only key | value columns, applying the prediction
        and send it to the 'processResponse' Kafka topic. 
    """
    stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", inputSchema).alias("data")) \
        .select("data.*") \
        .writeStream \
        .foreachBatch(elaborate) \
        .start() \
        .awaitTermination()

def main():
    stream = readStreamKafka(processTopics[0], brokers)
    foreachPredict(stream)

if __name__ == "__main__":
    main()
