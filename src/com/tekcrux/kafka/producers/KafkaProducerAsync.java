package com.tekcrux.kafka.producers;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class KafkaProducerAsync {
	public static void main(String[] args) throws Exception{
		
		  String topicName = "ccap_test-topic";
		
	      Properties props = new Properties();
	      props.put("bootstrap.servers","localhost:9092,localhost:9093,localhost:9093");
	      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      props.put("batch.size", 20480);
	      props.put("linger.ms", 1000);	        
	      
	      Producer<String, String> producer = new KafkaProducer <>(props);

	      System.out.println("Started writing messages.. ");
	      for (int i = 0; i < 10; i++) {		      	
		      producer.send( 
		    		         new ProducerRecord<>(topicName,"Key " + i, "Java KafkaProducerAsync Value " + i), 
		    		         new KafkaProducerAsyncCallback(i)
		    		       );
		     
		      System.out.println("Writing message to topic '" + topicName + "' >> - Java KafkaProducerAsync Value " + i);
	      }
	      System.out.println("AsynchronousProducer call completed");
	      producer.close();
	}
}

class KafkaProducerAsyncCallback implements Callback {

	private int key = 0;	
	
	public KafkaProducerAsyncCallback(int key) {
		this.key = key;
	}
	
    @Override
    public  void onCompletion(RecordMetadata metadata, Exception e) {
    	if (e != null) {
    		System.out.println("KafkaProducerAsync failed with an exception");    		
    	}
        else {
        	System.out.println("Calling KafkaProducerAsyncCallback.onCompletion method");
        	
        	String strMetaData = "partition: " + metadata.partition() +
		  			"; topic: " + metadata.topic() + 
		  			"; offset: " + metadata.offset() +
		  			"; hashCode: " + metadata.hashCode() + 
		  			"; timestamp: " + metadata.timestamp() + 
		  			"; key : " + key ;
		  			
        	System.out.println(strMetaData);            
        }
    }
}
