package io.guberlo.sapere.consumer.product;

import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import com.johnsnowlabs.nlp.annotators.classifier.dl.DeBertaForTokenClassification;
import com.johnsnowlabs.nlp.annotators.classifier.dl.RoBertaForSequenceClassification;
import com.johnsnowlabs.nlp.annotators.ner.NerConverter;
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
import io.guberlo.sapere.consumer.model.Consumer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class SparkNLPConsumer extends Consumer {

    private Pipeline pipeline;

    public SparkNLPConsumer(String configPath, String label) {
        super(configPath, label);
        this.pipeline = setPipeline();
    }

    @Override
    public void elaborate(Dataset<Row> dataset, Long datasetId) throws StreamingQueryException, TimeoutException {
        SparkSession session = SparkSession.builder().getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String outputTopic = config.getKafkaConfig().getProcessTopic().get(1);
        String broker = config.getKafkaConfig().getBrokers();

        if (dataset.isEmpty())
            return;

        pipeline.fit(dataset).transform(dataset)
                .select(functions.col("id"), functions.lit("SparkNLP_NER").as("type"), functions.col("text"), functions.col("prediction").cast("string"))
                .select(functions.to_json(functions.struct("id", "type", "text", "prediction")).alias("value"))
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("topic", outputTopic)
                .save();
    }

    @Override
    public Row predict(Row row) {
        return null;
    }

    private static Pipeline setPipeline() {
        DocumentAssembler documentAssembler = new DocumentAssembler();
        documentAssembler.setInputCol("text");
        documentAssembler.setOutputCol("document");

        Tokenizer tokenizer = new Tokenizer();
        tokenizer.setInputCols(new String[] {"document"});
        tokenizer.setOutputCol("token");

        DeBertaForTokenClassification tokenClassifier = (DeBertaForTokenClassification) DeBertaForTokenClassification.load("/opt/spark-3.1.1-bin-hadoop2.7/deberta_v3_base_token_classifier_ontonotes_en_3.4.4_3.0_1651826367819");
        tokenClassifier.setInputCols(new String[]{"document", "token"});
        tokenClassifier.setOutputCol("ner");
        tokenClassifier.setCaseSensitive(true);
        tokenClassifier.setMaxSentenceLength(512);

        NerConverter nerConverter = new NerConverter();
        nerConverter.setInputCols(new String[]{"document", "token", "ner"});
        nerConverter.setOutputCol("prediction");

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{documentAssembler, tokenizer, tokenClassifier, nerConverter});
        return pipeline;
    }
}
