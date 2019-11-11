package com.example.kafka_streams_examples;

import java.util.Properties;
import java.util.StringJoiner;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

public class TraFilterExample {
	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("tra-filter");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		flights
		.mapValues(new ValueMapper<String, String>() {
			@Override
			public String apply(String value) {
				String[] words = value.split(KafkaConstants.SEPARATOR);
				StringJoiner joiner = new StringJoiner(KafkaConstants.SEPARATOR);
				joiner.add(words[0]);
				joiner.add(words[3]);
				joiner.add(words[5]);
				joiner.add(words[9]);
				return joiner.toString();
			}
		})
		.filterNot((key, value) -> value.split(KafkaConstants.SEPARATOR)[1].isEmpty())
		.to(KafkaConstants.TOPIC_NAME + "-tra");

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
