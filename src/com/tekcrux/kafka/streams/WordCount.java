package com.tekcrux.kafka.streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class WordCount {
	public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1000");

        KStreamBuilder builder = new KStreamBuilder ();
        KStream<String, String> source = builder.stream("wordcount-topic");

        final Pattern pattern = Pattern.compile("\\W+"); 

        KStream<String, Long> counts = source
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((k, v) -> KeyValue.pair(v, v))
                .filter((k, v) -> !v.equals("the"))
                .groupByKey()
                .count()
                .toStream();
        
        counts.to(Serdes.String(),Serdes.Long(),"wordcount-result");

        KafkaStreams streams = new KafkaStreams(builder,props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
