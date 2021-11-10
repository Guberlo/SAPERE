package io.guberlo.sapere.product;

import io.guberlo.sapere.model.Consumer;
import org.apache.spark.sql.Row;

public class ConcreteConsumer extends Consumer {

    public ConcreteConsumer(String configPath, String label) {
        super(configPath, label);
    }

    @Override
    public Row predict(Row row) {
        String[] words = row.getAs("text").toString().split("\\s+");
        String prediction = String.valueOf(words.length);

        return toRow(row, prediction);
    }
}
