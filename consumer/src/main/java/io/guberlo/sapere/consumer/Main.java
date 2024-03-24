package io.guberlo.sapere.consumer;

import io.guberlo.sapere.consumer.model.Consumer;
import io.guberlo.sapere.consumer.product.ConcreteConsumer;
import io.guberlo.sapere.consumer.product.JeroConsumer;
import io.guberlo.sapere.consumer.product.NERConsumer;
import io.guberlo.sapere.consumer.product.SparkNLPConsumer;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws StreamingQueryException, TimeoutException, IOException {
        Consumer consumer = new SparkNLPConsumer("/opt/app/config.yaml", "SparkNLP_NER");
        LOG.debug("Consumer configuration path: {} ", consumer.getConfigPath());
        LOG.debug(consumer.getConfig().toString());

        if (consumer.getConfig().getKafkaConfig() != null && consumer.getConfig().getElasticConfig() != null)
            consumer.start();
        else
           LOG.error("Consumer has not correctly been loaded. Shutting down the application");
    }
}
