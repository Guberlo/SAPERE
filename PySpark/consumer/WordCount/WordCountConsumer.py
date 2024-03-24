from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql import types as tp
from pyspark.sql import Row
from pyspark.sql.functions import struct, to_json, from_json

from KafkaUtils import readStreamKafka, writeStreamKafka # imported using --py-files
from ConfigUtils import getYaml
from SparkUtils import inputSchema, outputSchema, writeToConsole

import json
import nltk

nltk.download('stopwords')

from nltk.corpus import stopwords
stopw = set(stopwords.words('english'))

cfg = getYaml("/opt/spark/config/config.yaml")

processTopics = cfg['kafka']['processTopics']
brokers = cfg['kafka']['brokers']
label = "wordcount"

context = SparkContext(appName="Sentiment Consumer")
context.setLogLevel("WARN")
session = SparkSession(context)

def predict(data: Row) -> Row:
    """
        Count the occurences of each word in the passed text.
        The prediction is returned as a JSON with {'word': number_of_occurences}.

        Return a Row containing {id: Elastic_Search_id, type: Job_Label, text, prediction: result_of_the_prediction}.
    """
    tokens = [t for t in data.text.split()]
    clean_tokens = tokens[:]

    for token in tokens:
        if token in stopw:
            clean_tokens.remove(token)

    values = []
    freq = nltk.FreqDist(clean_tokens)

    for key,val in freq.items():
        values.append({ key: val })
    
    return Row(id = str(data.id), type = label, text = str(data.text), prediction = json.dumps(values))

# TODO: maybe common function, some problem with the format after the map function
def elaborate(batch_df: DataFrame, batch_id: int):
    """
        Apply prediction and send the result to the 
        'processResponse' Kafka topic as output.
    """
    rdd = batch_df.rdd \
            .map(predict)

    if rdd.isEmpty():
        print("********* RDD EMPTY *********")
        return

    # TODO: common function
    df = session.createDataFrame(rdd, outputSchema)
    df.select(to_json(struct("id", "type", "text", "prediction")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", processTopics[1]) \
        .save()

# TODO: common function
def writeToConsole(stream: DataFrame):
    """
        Write the passed DataFrame to the console.
        Use only for test purposes.
    """
    stream.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

# TODO: common function
def foreachPredict(stream: DataFrame):
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
    stream = readStreamKafka()
    foreachPredict(stream)

if __name__ == "__main__":
    main()
