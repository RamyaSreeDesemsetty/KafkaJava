package com.tekcrux.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerFF2 {
	public static void main(String[] args) throws Exception{
 	   
		  String topicName = "topic-3";
		  
	      Properties kafkaProps = new Properties();
	      kafkaProps.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
	      kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
	      kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      
	      //more optional properties....
	      //kafkaProps.put("acks", "1");   //"0" -No ack, "1" only Leader ,"all" ALL
	      //kafkaProps.put("retries", 0);  // "0" doesn't re try ; positive value will retry
	      //kafkaProps.put("buffer.memory", 104857600);
	      //kafkaProps.put("max.block.ms", 30000);
	      //kafkaProps.put("batch.size", 16384);
	      //kafkaProps.put("linger.ma", 5000);
		        
	      Producer<String, String> producer = new KafkaProducer <>(kafkaProps);				  		   
		  
	      System.out.println("Starting KafkaProducerFF ...");
	      		   
		  String key = "";
		  String msg = "";
		  for (int i = 0; i < 50; i++) {
			  try {
				  key = "MyKey" + i%2 ; 
				  msg = "Message " + i + " from KafkaProducerFF2 with key: " + key;
				  producer.send(new ProducerRecord<>(topicName, key, msg)); 
				  System.out.println("Sent message >> " + msg);
			  }
			  catch (Exception e) { 
				  e.printStackTrace();
			  }
			  Thread.sleep(5000);
		  }
		  		  
		  producer.close();
	 }
}
