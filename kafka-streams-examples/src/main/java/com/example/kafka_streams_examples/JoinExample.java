package com.example.kafka_streams_examples;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

public class JoinExample {
	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("per-day-counter");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		final KStream<String, String> codes = builder.stream("codes", Consumed.with(Serdes.String(), Serdes.String()));

		flights
		.map((key, value) -> new KeyValue<>(value.split(KafkaConstants.SEPARATOR)[0], value))
		.join(
			codes, (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
    			JoinWindows.of(Duration.ofMinutes(5)),
    			Joined.with(
					Serdes.String(), /* key */
					Serdes.String(),   /* left value */
					Serdes.String())  /* right value */
		)
		.print(Printed.toSysOut());
		//.to(KafkaConstants.TOPIC_NAME + "-per-day", Produced.with(Serdes.String(), Serdes.Long()));

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
