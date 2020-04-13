package com.github.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        String bootstrapServers = "127.0.0.1:9092" ;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i=0; i <= 50; i++) {

            String topic = "twitter-tweets" ;
            String value = "Mensagem " + Integer.toString(i);
            String key = "Key " + Integer.toString(i);

            ProducerRecord<String, String> record =  new ProducerRecord<String, String>(topic, key, key);

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n" );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        // sudo ./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic FIRST_TOPIC --group MY_THIRD_CONSUMER
    }
}
