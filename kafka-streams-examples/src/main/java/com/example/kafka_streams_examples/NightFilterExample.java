package com.example.kafka_streams_examples;

import java.util.Properties;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

public class NightFilterExample {
	private static final DateTimeFormatter DATE_FORMATER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public static void main(final String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("night-filter");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		flights
		.filter((key, value) -> {
			final String[] words = value.split(KafkaConstants.SEPARATOR);
			final LocalDateTime departureDateTime = LocalDateTime.parse(words[6], DATE_FORMATER);
			final int hour = departureDateTime.getHour();

			return hour >= 1 && hour < 7;
		})
		.print(Printed.toSysOut());
		//.to(KafkaConstants.TOPIC_NAME + "-night");

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
