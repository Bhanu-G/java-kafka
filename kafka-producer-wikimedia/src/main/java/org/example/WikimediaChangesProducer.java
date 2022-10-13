package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private final static Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // if the kafka client version is 3.0.0, for idempotent producer
    /*  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "5");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "12000");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    */
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(1);
    }
}