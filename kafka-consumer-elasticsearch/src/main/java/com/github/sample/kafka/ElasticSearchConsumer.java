package com.github.sample.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-lab-5890639845.us-east-1.bonsaisearch.net";
        String username = "1ju5dhevln";
        String password = "evqp2mee7y";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();
//        String jsonString  = " { \"course \": \"java\"   }" ;
//        IndexRequest indexRequest = new IndexRequest(
//                "twitter"
//        ).source(jsonString, XContentType.JSON)  ;

//        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//        String id  = indexResponse.getId();
//        logger.info(id);
        KafkaConsumer<String, String> consumer =  createConsumer("twitter-tweets");

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(100);

        for (ConsumerRecord record : records ) {
                // Kafka generic Id
//                String id_elastic = extractIdFromTweet(record.value().toString());

                IndexRequest indexReq = new IndexRequest(
                        "twitter"
                ).source(record.value(), XContentType.JSON);

                IndexResponse indexResp = client.index(indexReq, RequestOptions.DEFAULT);
                logger.info(indexResp.getId());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
             }
         }
       // client.close();
    }

//    private static String extractIdFromTweet(String tweetJson) {
//          return jsonParser.parse(tweetJson)
//                .getAsJsonObject()
//                .get("id_str")
//                .getAsString();
//    }

    public static KafkaConsumer<String, String> createConsumer (String topic) {

        String bootstrapServers = "127.0.0.1:9092" ;
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // redefinição de deslocamento automático
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
}