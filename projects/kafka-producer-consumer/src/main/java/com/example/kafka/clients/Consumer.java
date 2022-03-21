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
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // bootstrap.servers 설정
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key.deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value.deserializer
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic); // group.id

        this.consumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(String topic) {
        consumer.subscribe(Collections.singletonList(topic));

        String message = null;
        try {
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // partition 에 레코드가 있는 경우 즉시 가져온다. 레코드가 없으면 timeout 동안 기다린다. timeout 이 초과하면 빈 리스트를 반환한다.
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
