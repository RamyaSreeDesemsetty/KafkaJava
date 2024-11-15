package com.tekcrux.custom_partitioner;

import java.util.Objects;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import com.tekcrux.kafka.*;

public class OrderPartitioner extends DefaultPartitioner {
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
	      Cluster cluster) {
        
	    String partitionKey = null;
	    
	    if (Objects.nonNull(key)) {
	      Order orderKey = (Order) key;
	      partitionKey = orderKey.getCustomerId();
	      keyBytes = partitionKey.getBytes();
	    }
	    return super.partition(topic, partitionKey, keyBytes, value, valueBytes, cluster);	    	        
	}
}
