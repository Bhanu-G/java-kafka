package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithCooperative {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithCooperative.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Consumer running");
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "first_consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ""); // static group membership
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected a shutdown.");
                consumer.wakeup();

                // join the main thread to allow the execution of code in main thread
                try {
                    mainThread.join();
                    logger.info("Continued to exit the program, Graceful shutdown!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        // subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList("demo_topic"));
        try {
            // polling for new data
            while (true) {
                logger.info("Polling...");
                ConsumerRecords<String, String> reocrds = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : reocrds) {
                    logger.info("Conusmeed new data. \n" +
                            "Topic: " + record.topic() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "Timestamp: " + record.timestamp() + "\n");
                }
            }
        } catch (WakeupException we) {
            logger.info("Wakeup exception");
        } catch (Exception e) {
            logger.error("Error while consuming from kafka");
        } finally {
            consumer.close();
        }
    }
}
