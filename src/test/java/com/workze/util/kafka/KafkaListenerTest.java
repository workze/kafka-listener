package com.workze.util.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaListenerTest {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        KafkaListener<String, String> listener = new KafkaListener<String, String>()
                .properties(properties)
                .topics(Collections.singletonList("test"))
                .valueHandler((v) -> {
                    System.out.println(v);
                });
        listener.start();
        TimeUnit.SECONDS.sleep(60);
        listener.stop();
    }


}