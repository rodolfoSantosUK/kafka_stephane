package com.github.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    public static void main(String[] args) {
        new ConsumerWithThread().run();
    }

    private ConsumerWithThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);
        String bootstrapServers = "127.0.0.1:9092" ;
        String groupId = "MY_FIFTH-APPLICATION";
        String topic = "FIRST_TOPIC";

        //para lidar com multiplas threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        // cria o consumer
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));


        try {
            latch.await(); // aquarda todo o processo ate o fim da aplicação
        } catch (InterruptedException e) {
            logger.error("Application got interrupted ", e);
        } finally {
            logger.info("*** Application is closing ***");
        }

    }

public class ConsumerRunnable implements  Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // significa que quer ler desde o inicio
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);

                    for (ConsumerRecord record : records ) {
                        logger.info(" ***** MENSAGEM *****");
                        logger.info(" Key: " + record.key() );
                        logger.info(" Value: " + record.value() );
                        logger.info(" Partition: " + record.partition() );
                        logger.info(" Offset: " + record.offset() );
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // Diz para o método principal que terminamos com o consumer
                latch.countDown();
            }
        }
        public void shutdown() {
            // é um metodo especial para interromper consumer.poll()
            // vai lançar uma WakeUpException
            consumer.wakeup();
        }
     }
}
