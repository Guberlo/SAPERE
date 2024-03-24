import yaml

from typing import Iterator

from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, udf, to_json, struct
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import types as tp

from pyspark.ml.feature import StopWordsRemover, Tokenizer

from KafkaUtils import readStreamKafka, writeStreamKafka # imported using --py-files
from ConfigUtils import getYaml
from SparkUtils import inputSchema, outputSchema, writeToConsole

from elasticsearch import Elasticsearch

cfg = getYaml("/opt/spark/config/config.yaml")

brokers = cfg['kafka']['brokers']
topic = cfg['kafka']['generalTopic']
processTopic = cfg['kafka']['processTopics']

elasticHost = cfg['elastic']['host']
index = cfg['elastic']['index']

context = SparkContext(appName="Requests Dispatcher")
session = SparkSession(context)
lang = "english"

context.setLogLevel("WARN")

tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
countTokens = udf(lambda words: len(words), tp.IntegerType())

stopWords = StopWordsRemover.loadDefaultStopWords(lang)
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered", stopWords=stopWords)

print(f"Spark version = {context.version}")
print(f"Hadoop version = {context._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

def preprocessing(stream: DataFrame) -> DataFrame:
    """
        Take JSON data from Kafka and apply some basic NLP,
        if requested.

        Keyword argument:
        stream -- DataFrame containing data from API.
    """
    request = 0

    # Fit DataFrame from kafka to schema specified above and removes NA
    words = stream.selectExpr("CAST(value AS STRING)") \
            .alias("json")
    words = words.select(from_json(words.value, inputSchema).alias("data")) \
            .select("data.*")
    words = words.na.drop()

    if not words:
        print("No Data")
        return 

    # Apply tokenization/stopwords if specified
    if request == 1:
        tokenized = tokenize(words)
        stopRemoved = removeStopWords(tokenized)
        return stopRemoved
    
    return words

def tokenize(words: DataFrame) -> DataFrame:
    """
        Apply tokenization using spark ML feature.
    """
    tokenized = tokenizer.transform(words)
    return tokenized

def removeStopWords(tokens: DataFrame) -> DataFrame:
    """
        Remove stop words using spark ML feature.
    """
    stopRemoved = remover.transform(tokens)
    return stopRemoved

def func(batch_df: DataFrame, batch_id: int) -> None:
    """
        Save each batch into Elastic Search updating the 
        Already existing resource with new predictions.
    """
    rdd = batch_df.foreachPartition(saveToElastic)
    
def saveToElastic(data: Iterator) -> None:
    """
        Update the corresponding Elastic Search resource
        With the corresponding prediction. 
    """
    es = Elasticsearch(hosts=[elasticHost]) # We need to create a new ES because it is not de-serializable and can't be passed to a spark executor
    for doc in data:
        id = doc.id
        es.update(index=index, id=id, body={"doc": {doc.type:doc.prediction}})

def sendToElastic(stream: DataFrame) -> None:
    """
        Parse the received DataFrame into the appropriate schema
        And apply a function to save data into Elastic Search with
        The corrisponding predictions.
    """
    stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", outputSchema).alias("data")) \
        .select("data.*") \
        .writeStream \
        .foreachBatch(func) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    # Read stream from kafka to a DataFrame
    stream = readStreamKafka(topic, brokers)
    words = preprocessing(stream)
    words.printSchema()

    writeStreamKafka(words, processTopic[0], brokers)

    out = readStreamKafka(processTopic[1], brokers)
    sendToElastic(out)
