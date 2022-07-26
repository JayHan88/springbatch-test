package com.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class ProducerMain {

    public static void main(String[] args) {

        try {

            // set kafka properties
            Properties configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // kafka cluster에 연결하기 위한 broker list
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // message key serialization
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // message value serialization

            // init kafka producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

            int idx = 0;
            while(true) {

                // set producer record
                String topic = "test"; // topic name
                Integer partition = 0; // partition number
                String key = "key-" + idx; // key
                String data = "record-" + idx; // data

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);
                ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, key, data);
                ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, partition, key, data);

                // send record
                producer.send(record);

                System.out.println("producer.send() >> [topic:" + topic + "][data:" + data + "]");
                Thread.sleep(1000);
                idx++;
            }
        } catch (Exception e) {
            System.out.println(e);

        }
    }
}
