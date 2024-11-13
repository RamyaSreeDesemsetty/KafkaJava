package com.tekcrux.kafka.streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import com.google.gson.Gson;
import com.tekcrux.kafka.Invoice;

public class StatelessStream {
	
	public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StatelessStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");

        KStreamBuilder builder = new KStreamBuilder ();
        KStream<String, String> source = builder.stream("invoices");

        Gson gson = new Gson();
        
        KStream<String, Invoice> t1 = source.mapValues(str -> gson.fromJson(str, Invoice.class));
        
        t1.mapValues(invoice -> 
        	{
			    if(invoice.getInvoiceAmount() > 1000) { invoice.setDiscount(5); }
			    return gson.toJson(invoice);
        	})
        .to("discounted-invoices");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.start();
                
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
