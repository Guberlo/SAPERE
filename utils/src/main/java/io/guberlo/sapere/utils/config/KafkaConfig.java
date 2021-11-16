package io.guberlo.sapere.utils.config;

import java.util.List;

public class KafkaConfig {
    private String brokers;
    private String generalTopic;
    private List<String> processTopic;

    public KafkaConfig(String brokers, String generalTopic, List<String> processTopic) {
        this.brokers = brokers;
        this.generalTopic = generalTopic;
        this.processTopic = processTopic;
    }

    public KafkaConfig() {
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getGeneralTopic() {
        return generalTopic;
    }

    public void setGeneralTopic(String generalTopic) {
        this.generalTopic = generalTopic;
    }

    public List<String> getProcessTopic() {
        return processTopic;
    }

    public void setProcessTopic(List<String> processTopic) {
        this.processTopic = processTopic;
    }

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "brokers='" + brokers + '\'' +
                ", generalTopic='" + generalTopic + '\'' +
                ", processTopic=" + processTopic +
                '}';
    }
}
