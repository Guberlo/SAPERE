from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql import types as tp
from pyspark.sql.dataframe import DataFrame
import spacy

nlp = spacy.load('en_core_web_sm')
brokers = "10.0.100.23:9092"
topic = "ner"

context = SparkContext(appName="NER Consumer")
session = SparkSession(context)

context.setLogLevel("WARN")

outputSchema = tp.StructType([
    tp.StructField(name='key', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='text', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='pos', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='tag', dataType=tp.StringType(), nullable=False),
])

def tagNER(tokens: DataFrame) -> RDD:
    """
        Apply NER tags to the requested sentence and returns a key: value obj.
    """
    i = 1
    ner = {}
    doc = nlp(tokens.value)

    for token in doc:
        ner.update({'key{number}'.format(number = i) : {'text': token.text, 'pos': token.pos_, 'tag': token.tag_} })
        i = i+1
    
    return ner

def func(batch_df: DataFrame, batch_id: int) -> None:
    """
        Apply tags and show the results.

        Results should then be sent somewhere else.
    """
    rdd = batch_df.rdd \
            .map(tagNER)

    if rdd.isEmpty():
        print("********* RDD EMPTY *********")
        return

    df = session.createDataFrame(rdd)
    df.show()

if __name__ == "__main__":
    # This loads a DataStream into a DataFrame
    stream = session.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", brokers) \
                .option("subscribe", topic) \
                .load()

    # Print results into console
    stream.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreachBatch(func) \
        .start() \
        .awaitTermination()