package com.tekcrux.kafka.streams;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class StatefulConsumer {
	
	public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "grp-1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("stateful-result"));

        System.out.println("Starting Consumer ...");
        try {
            while (true) {
                ConsumerRecords<String, Long> records = consumer.poll(2000);
                for (ConsumerRecord<String, Long> record : records) {
                    System.out.println("key = " + record.key() + "  value = " + record.value());
                }
            }
        }
        finally {
            consumer.close();
        }
    }
}
