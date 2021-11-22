package io.guberlo.sapere.utils.elastic;

import org.apache.http.HttpHost;
import org.apache.spark.sql.Row;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

public class ElasticUtils implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(ElasticUtils.class);
    private transient static RestHighLevelClient client;
    private final String index;

    public ElasticUtils(String index, String host) {
        this.index = index;
        try {
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost(host, 9200, "http")
            ));
        } catch (Exception e) {
            LOG.error("Connecting to Elastic Search: {}", e.getMessage());
        }
    }

    public void updateOnES(Iterator<Row> data) {
        data.forEachRemaining(row -> {
            String id = row.getAs("id");
            String type = row.getAs("type");
            String prediction = row.getAs("prediction");
            UpdateRequest request = new UpdateRequest(index, id);
            String body = "{" +
                    "\"" + type + "\":" +
                    "\"" + prediction.replaceAll("\"", "'") + "\"" +
                    "}";

            request.doc(body, XContentType.JSON);

            try {
                UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
                LOG.info("Elastic Search update info: {}", updateResponse.getGetResult());
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    LOG.error("Conflict on Elastic Search update");
                }
            } catch (IOException e) {
                LOG.error("Error on updating elastic search doc: {}", e.getMessage());
                LOG.error("Request body is: {}", body);
            }
        });
    }
}
