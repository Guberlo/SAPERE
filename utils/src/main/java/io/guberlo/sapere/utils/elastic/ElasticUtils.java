package io.guberlo.sapere.utils.elastic;

import org.apache.http.HttpHost;
import org.apache.spark.sql.Row;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class ElasticUtils implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(ElasticUtils.class);
    private transient static RestHighLevelClient client;
    private final String index;
    private final String mapping = readMappingV2();

    public ElasticUtils(String index, String host) {
        this.index = index;
        try {
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost(host, 9200, "http")
            ));
            createIndex();
        } catch (Exception e) {
            LOG.error("Connecting to Elastic Search: {}", e.getMessage());
        }
    }

    public String readMappingV1() {
        return "{\n" +
                "    \"properties\": {\n" +
                "      \"document_id\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"text\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"join_field\": {\n" +
                "        \"type\": \"join\",\n" +
                "        \"relations\": {\n" +
                "          \"document\": [\n" +
                "            \"alg1\",\n" +
                "            \"alg2\",\n" +
                "            \"alg3\",\n" +
                "            \"gero\"\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"alg1\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"alg2\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"alg3\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"gero\": {\n" +
                "        \"type\": \"nested\",\n" +
                "        \"properties\": {\n" +
                "          \"start\": {\n" +
                "            \"type\": \"integer\"\n" +
                "          },\n" +
                "          \"end\": {\n" +
                "            \"type\": \"integer\"\n" +
                "          },\n" +
                "          \"mention\": {\n" +
                "            \"type\": \"keyword\"\n" +
                "          },\n" +
                "          \"entity_id\": {\n" +
                "            \"type\": \"integer\"\n" +
                "          },\n" +
                "          \"entity_name\": {\n" +
                "            \"type\": \"keyword\"\n" +
                "          },\n" +
                "          \"entity_type\": {\n" +
                "            \"type\": \"keyword\"\n" +
                "          },\n" +
                "          \"rho\": {\n" +
                "            \"type\": \"double\"\n" +
                "          },\n" +
                "          \"confidence\": {\n" +
                "            \"type\": \"double\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    public String readMappingV2() {
        return "{\n" +
                "    \"properties\": {\n" +
                "      \"text\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"predictions\": {\n" +
                "          \"type\": \"nested\",\n" +
                "          \"properties\": {\n" +
                "            \"alg1\": {\n" +
                "                \"type\": \"keyword\"\n" +
                "              },\n" +
                "              \"alg2\": {\n" +
                "                \"type\": \"keyword\"\n" +
                "              },\n" +
                "              \"alg3\": {\n" +
                "                \"type\": \"keyword\"\n" +
                "              },\n" +
                "              \"gero\": {\n" +
                "                \"type\": \"nested\",\n" +
                "                \"properties\": {\n" +
                "                  \"start\": {\n" +
                "                    \"type\": \"integer\"\n" +
                "                  },\n" +
                "                  \"end\": {\n" +
                "                    \"type\": \"integer\"\n" +
                "                  },\n" +
                "                  \"mention\": {\n" +
                "                    \"type\": \"keyword\"\n" +
                "                  },\n" +
                "                  \"entity_id\": {\n" +
                "                    \"type\": \"integer\"\n" +
                "                  },\n" +
                "                  \"entity_name\": {\n" +
                "                    \"type\": \"keyword\"\n" +
                "                  },\n" +
                "                  \"entity_type\": {\n" +
                "                    \"type\": \"keyword\"\n" +
                "                  },\n" +
                "                  \"rho\": {\n" +
                "                    \"type\": \"double\"\n" +
                "                  },\n" +
                "                  \"confidence\": {\n" +
                "                    \"type\": \"double\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "          }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    public void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index)
                .mapping(mapping, XContentType.JSON);

        client.indices().create(request, RequestOptions.DEFAULT);
    }

    public void updateOnES(Iterator<Row> data) {
        data.forEachRemaining(row -> {
            String id = row.getAs("id");
            String type = row.getAs("type");
            String prediction = row.getAs("prediction");
            LOG.warn(prediction);

            UpdateRequest request = new UpdateRequest(index, id);
            String body = "{" +
                    "\"predictions\": {" +
                        "\"" + type + "\":" +
                        prediction +
                    "}" +
                    "}";

            LOG.warn(body);

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
