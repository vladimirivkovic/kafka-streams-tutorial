package com.example.kafka_streams_examples;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class BranchByCraftExample {
	private static final String SEPARATOR = "#";

	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("branch-craft");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		KStream<Long, String>[] branches = flights.branch(
				(key, value) -> value.split(KafkaConstants.SEPARATOR)[1].startsWith("3"), /* first predicate */
				(key, value) -> value.split(KafkaConstants.SEPARATOR)[1].startsWith("7"), /* second predicate */
				(key, value) -> true /* third predicate */);
		
		branches[0].to(KafkaConstants.TOPIC_NAME + "-craft-3");
		branches[1].to(KafkaConstants.TOPIC_NAME + "-craft-7");
		branches[2].to(KafkaConstants.TOPIC_NAME + "-craft-X");

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
