package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String> producer;
    private String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() { }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info("Wikimedia Stream " + messageEvent.getData());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(record);
    }

    @Override
    public void onComment(String s) { }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error while streaming the wikimedia changes!");
    }
}
