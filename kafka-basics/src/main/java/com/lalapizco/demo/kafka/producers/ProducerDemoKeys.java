package com.lalapizco.demo.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    private static final String demoTopicName = "demo_java";

    public static void main(String[] args) {
        log.info("Producer with Callback");

        Properties properties = getProducerProperties();

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j<2; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "Hello World " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(demoTopicName, key, value);

                //Send data
                producer.send(producerRecord, (metadata, e) -> {
                    if(e == null) {
                        log.info("Key: "+ key + " | Partition: " + metadata.partition());
                    } else {
                        log.error(e.getMessage());
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();
        producer.close();
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        //properties.setProperty("partinioner.class", RoundRobinPartitioner.class.getName());
        return properties;
    }
}
