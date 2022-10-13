package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Producer running!");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "Hello Kafka! " + i);
            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record sent successfully or exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: "+ recordMetadata.timestamp() + "\n");
                    }
                }
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                logger.error("Exception occured while send data to kafka. " + ie);
            }
        }
        // flush - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }
}