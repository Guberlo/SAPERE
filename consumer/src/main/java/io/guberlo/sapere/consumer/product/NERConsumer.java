package io.guberlo.sapere.consumer.product;

import com.google.gson.Gson;
import io.guberlo.sapere.consumer.model.Consumer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class NERConsumer extends Consumer {
    private static final Logger LOG = LoggerFactory.getLogger(NERConsumer.class);
    SerializableNameFinder nameFinder;

    public NERConsumer(String configPath, String label) throws IOException {
        super(configPath, label);
        nameFinder = new SerializableNameFinder();
    }

    @Override
    public Row predict(Row row) {
        try {
            NameFinderME model = nameFinder.getNERModel();
            SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
            HashMap<String, Object> entities = new HashMap<>();
            Gson gson = new Gson();
            ArrayList<HashMap<String, String>> openNERList = new ArrayList<>();
            String[] tokens = tokenizer.tokenize(row.getAs("text").toString()); // Tokenize the input text
            Span[] nameSpans = model.find(tokens); // Find person names

            LOG.warn("SPAN LENGTH: " + nameSpans.length);
            // Print recognized names
            for (Span nameSpan : nameSpans) {
                HashMap<String, String> entry = new HashMap<>();
                LOG.warn("NAMESPAN: {} ", nameSpan);
                String tag = nameSpan.toString().split(" ")[1];
                String personName = buildPersonName(tokens, nameSpan.getStart(), nameSpan.getEnd());
                entry.put("name", personName);
                entry.put("label", tag);
                openNERList.add(entry);
                LOG.warn("Classification: {" + personName + ": " + tag);
            }
            // Converting the HashMap to a JSON string
            entities.put("OpenNER", openNERList);
            String prediction = gson.toJson(entities);
            LOG.warn("JSON IS: {}", prediction);

            return toRow(row, prediction);
        } catch (Exception e) {
            LOG.error("EVERYTHING BROKE!!! {}", e.getMessage());
        }

        String prediction = "";
        return toRow(row, prediction);
    }

    private String buildPersonName(String[] tokens, Integer start, Integer end) {
        String name = "";
        for (int i = start; i < end; i++ ) {
            name = name.concat(" " + tokens[i]);
        }
        return name;
    }

}

class SerializableNameFinder implements Serializable {
    private NameFinderME model;
    public NameFinderME getNERModel() throws IOException {
        if (model == null) {
            FileInputStream nerModel = new FileInputStream("/opt/spark-3.1.1-bin-hadoop2.7/en-ner-person.bin");
            TokenNameFinderModel nameFinderModel = new TokenNameFinderModel(nerModel);
            nerModel.close();

            this.model = new NameFinderME(nameFinderModel);
        }

        return model;
    }

}