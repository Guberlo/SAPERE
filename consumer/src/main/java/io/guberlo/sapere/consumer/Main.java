package io.guberlo.sapere.consumer;

import io.guberlo.sapere.consumer.model.Consumer;
import io.guberlo.sapere.consumer.product.ConcreteConsumer;
import io.guberlo.sapere.consumer.product.JeroConsumer;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Consumer consumer = new JeroConsumer("/opt/app/config.yaml", "Jero");
        LOG.debug("Consumer configuration path: {} ", consumer.getConfigPath());
        LOG.debug(consumer.getConfig().toString());

        if (consumer.getConfig().getKafkaConfig() != null && consumer.getConfig().getElasticConfig() != null)
            consumer.start();
        else
           LOG.error("Consumer has not correctly been loaded. Shutting down the application");
    }
}
