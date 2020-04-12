package com.github.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers = "127.0.0.1:9092" ;
        String groupId = "MY_FOURTH-APPLICATION";
        String topic = "FIRST_TOPIC";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none"); // significa que quer ler desde o inicio

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        // Assign e seek são frequentemente usados para reprize de dados e obter uma mensagem específica

        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offSetReadFrom = 15L ;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offSetReadFrom);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord record : records ) {
                logger.info(" ***** VAI CONSUMIR NOVA MENSAGEM *****");
                logger.info(" Key: " + record.key() );
                logger.info(" Value: " + record.value() );
                logger.info(" Partition: " + record.partition() );
                logger.info(" Offset: " + record.offset() );
            }
        }
    }
}
