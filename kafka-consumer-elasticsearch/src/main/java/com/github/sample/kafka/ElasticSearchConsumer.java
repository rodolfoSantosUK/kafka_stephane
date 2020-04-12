package com.github.sample.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class ElasticSearchConsumer {

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
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String jsonString  = " { \"course \": \"java\"   }" ;

        IndexRequest indexRequest = new IndexRequest(
                "twitter"
        ).source(jsonString, XContentType.JSON)  ;

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id  = indexResponse.getId();
        logger.info(id);

        client.close();
    }
}
