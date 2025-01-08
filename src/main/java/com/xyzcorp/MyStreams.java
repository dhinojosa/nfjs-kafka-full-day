package com.xyzcorp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Properties;

public class MyStreams {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
            "my_stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.Integer().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> stream =
            builder.stream("my_orders"); //Key: State, Value: Amount

        //One branch
        stream.filter((key, value) -> key.equals("CA"))
              .to("priority_state_orders");

        //Second branch
        stream.groupByKey()
              .count()
              .toStream()
              .peek((key, value) ->
                  System.out.println(String.format("key: %s, value %d", key, value)))
              .to("state_orders_count",
                  Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}