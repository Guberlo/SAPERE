from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as tp

inputSchema = tp.StructType([
        tp.StructField(name='id', dataType=tp.StringType(), nullable=False),
        tp.StructField(name='text', dataType=tp.StringType(), nullable=False)
])

outputSchema = tp.StructType([
    tp.StructField(name='id', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='type', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='text', dataType=tp.StringType(), nullable=False),
    tp.StructField(name='prediction', dataType=tp.StringType(), nullable=False)
])

def writeToConsole(stream: DataFrame) -> None:
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