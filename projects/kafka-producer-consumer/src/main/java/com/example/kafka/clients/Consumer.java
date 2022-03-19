package com.example.kafka.clients;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    KafkaConsumer<String, String> consumer;

    public Consumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);

        this.consumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(String topic) {
        consumer.subscribe(Collections.singletonList(topic));

        String message = null;
        try {
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records) {
                    message = record.value();
                    System.out.println(message);
                }
            } while(!StringUtils.equals(message, "exit"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
