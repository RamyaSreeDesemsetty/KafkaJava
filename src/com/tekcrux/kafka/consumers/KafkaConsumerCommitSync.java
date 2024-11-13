package com.tekcrux.kafka.consumers;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerCommitSync {
	public static void main(String args[]) throws Exception{
		
		String topicName = "topic1";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");  
		props.put("group.id", "mygroup");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		Consumer<String, String> consumer = new KafkaConsumer<>(props);

		List<TopicPartition> partitions = new ArrayList<>(); 
        
		if (partitions != null) { 
			
			for (PartitionInfo partition : consumer.partitionsFor(topicName)) {
				partitions.add(new TopicPartition(topicName, partition.partition()));
			}				
			consumer.assign(partitions); 

			while (true) { 
				ConsumerRecords<String, String> records = consumer.poll(1000);
				 
				for (ConsumerRecord<String, String> record: records) { 
					
					System.out.printf( 
							"topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
							record.topic(), 
							record.partition(), 
							record.offset(), 
							record.key(), 
							record.value()); 
				}
				consumer.commitSync(); 	
			}			
		}
		consumer.close();
	}
}
