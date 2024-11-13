package com.tekcrux.kafka.streams;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

public class StatefulStream 
{
	public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StatefulStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1000");

        KStreamBuilder builder = new KStreamBuilder ();
        KStream<String, String> source = builder.stream("invoices");

        System.out.println("Starting the Stream...");
        
        KStream<String, Long> result = source
                .groupByKey()
                //.count(TimeWindows.of(60000).advanceBy(10000))
                //.count(TimeWindows.of(60000).advanceBy(10000).until(30000))
                .count(TimeWindows.of(60000))
                .toStream((k, v) -> k.key() + " -> [" + millisecondsToDateStr(k.window().start())+ "  to  " + millisecondsToDateStr(k.window().end()));

        result.to(Serdes.String(),Serdes.Long(),"stateful-result");

        KafkaStreams streams = new KafkaStreams(builder,props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    public static String millisecondsToDateStr(long ms)
    {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(ms), ZoneId.systemDefault());
        return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
