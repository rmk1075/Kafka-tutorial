package com.example.kafka.clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    KafkaProducer<String, String> producer;

    public Producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // bootstrap.servers 설정
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key.serializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value.serializer

        this.producer = new KafkaProducer<>(properties);
    }

    public void send(String topic, String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, message); // 메시지 전송에 사용할 record 객체 생성
            producer.send(record, (metadata, e) -> {
                if(e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(metadata);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
        }
    }

    public void close(String topic) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, "exit");
            producer.send(record, (metadata, e) -> {
                if(e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(metadata);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
        }
        producer.close();
    }
}
