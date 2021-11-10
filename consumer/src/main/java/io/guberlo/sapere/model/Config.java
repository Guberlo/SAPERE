package io.guberlo.sapere.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.guberlo.sapere.config.ElasticConfig;
import io.guberlo.sapere.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Config {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private KafkaConfig kafkaConfig;
    private ElasticConfig elasticConfig;

    public Config() {
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public ElasticConfig getElasticConfig() {
        return elasticConfig;
    }

    public void setElasticConfig(ElasticConfig elasticConfig) {
        this.elasticConfig = elasticConfig;
    }

    public Config getYaml(String path) {
        ObjectMapper om = new ObjectMapper(new YAMLFactory());

        try {
            return om.readValue(new File(path), Config.class);
        } catch (IOException e) {
            LOG.error("{} | this file couldn't be read!", path);
            return new Config();
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "kafkaConfig=" + kafkaConfig +
                ", elasticConfig=" + elasticConfig +
                '}';
    }
}
