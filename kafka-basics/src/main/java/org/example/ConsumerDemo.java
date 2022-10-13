package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Consumer running");
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "first_consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList("demo_topic"));
        // polling for new data
        while(true) {
            logger.info("Polling...");
            ConsumerRecords<String, String> reocrds = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : reocrds) {
                logger.info("Conusmeed new data. \n" +
                        "Topic: " + record.topic()+ "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Timestamp: "+ record.timestamp() + "\n");
            }
            // auto.commit.interval.ms = 5000 - default offset commit interval
            // consumer.commitAsync(); when enable.auto.commit = false
        }
        // close conumser
        // consumer.close(); - Unreacheble statement
    }
}
