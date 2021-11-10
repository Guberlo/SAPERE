package io.guberlo.sapere.config;

public class ElasticConfig {
    private String host;
    private String index;

    public ElasticConfig() {
    }

    public ElasticConfig(String host, String index) {
        this.host = host;
        this.index = index;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "ElasticConfig{" +
                "host='" + host + '\'' +
                ", index='" + index + '\'' +
                '}';
    }
}
