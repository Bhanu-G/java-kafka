package org.example;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.queries.function.valuesource.DualFloatFunction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private final static Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        // create consumer
        KafkaConsumer<String, String> consumer = createConsumer();
        // create openSearch client
        RestHighLevelClient openSearchClient = restHighLevelClient();
        // create index in open seach if it doesn't exist already
        try (openSearchClient; consumer) {
            boolean isIndexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!isIndexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The wikimedia index has been created!");
            } else {
                logger.info("OpenSearch index already existed with name: wikimedia");
            }
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                int recordCount = records.count();
                logger.info("Received " + recordCount + " records.");
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    // to make consumer idempotent
                    // String id = record.topic() + record.partition() + record.offset();
                    String id = extractId(record);
                    // send the record to OpenSearch
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
                    // IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    // logger.info("Indexed document id: " + response.getId());

                    bulkRequest.add(indexRequest);
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Records saved in OpenSearch: " + bulkResponse.getItems().length);
                    // wait more time to get larger batch records
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {

                    }
                    consumer.commitAsync();
                    logger.info("Offsets have been committed!");
                }
            }

        }
    }

    private static String extractId(ConsumerRecord<String, String> record) {
        String id = JsonParser.parseString(record.value()).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
        logger.debug(id);
        return id;
    }

    private static RestHighLevelClient restHighLevelClient() {
        String esUrl = "http://localhost:9200/";
        RestHighLevelClient restHighLevelClient;
        // build URI from connection string
        URI connUri = URI.create(esUrl);
        // extract login info if it exists
        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort())));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");
            CredentialsProvider credProvider = new BasicCredentialsProvider();
            credProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<String, String>(properties);
    }
}