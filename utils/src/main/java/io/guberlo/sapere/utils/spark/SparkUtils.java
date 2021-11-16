package io.guberlo.sapere.utils.spark;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class SparkUtils {

    private final static Logger LOG = LoggerFactory.getLogger(SparkUtils.class);

    private final StructType inputSchema;
    private final StructType outputSchema;
    private final StopWordsRemover stopWordsRemover;
    private final Tokenizer tokenizer;
    private String lang;

    public SparkUtils(String lang) {
        this.lang = lang;
        outputSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),
                new StructField("prediction", DataTypes.StringType, false, Metadata.empty())
        });

        inputSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });

        stopWordsRemover = new StopWordsRemover()
                .setInputCol("token")
                .setOutputCol("filtered")
                .setStopWords(StopWordsRemover.loadDefaultStopWords(lang));

        tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("tokens");
    }

    public StructType getInputSchema() {
        return inputSchema;
    }

    public StructType getOutputSchema() {
        return outputSchema;
    }

    public void writeToConsole(Dataset<Row> dataset) throws StreamingQueryException, TimeoutException {
        dataset.selectExpr("CAST(value AS STRING)")
                .writeStream()
                .format("console")
                .start()
                .awaitTermination();
    }

    public Dataset<Row> tokenize(Dataset<Row> dataset) {
        return tokenizer.transform(dataset);
    }

    public Dataset<Row> removeStopWords(Dataset<Row> dataset) {
        return stopWordsRemover.transform(dataset);
    }

}
