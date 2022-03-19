package com.example.kafka;

import com.example.kafka.clients.Consumer;
import com.example.kafka.clients.Producer;

public class App 
{
    static String TOPIC = "test01";
    public static void main( String[] args )
    {
        Producer producer = new Producer();
        Consumer consumer = new Consumer(TOPIC);
        for(int i = 0; i < 10; i++) {
            System.out.println(i);
            producer.send(TOPIC, String.format("test01 - message %d", i));
        }
        producer.close(TOPIC);
        consumer.subscribe(TOPIC);
    }
}
