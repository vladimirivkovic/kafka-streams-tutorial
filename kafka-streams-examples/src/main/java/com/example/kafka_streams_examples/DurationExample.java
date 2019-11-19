package com.example.kafka_streams_examples;

import java.util.Properties;
import java.util.StringJoiner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

public class DurationExample {
	private static final DateTimeFormatter DATE_FORMATER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("duration");

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
				LocalDateTime departureDateTime = LocalDateTime.parse(words[5], DATE_FORMATER);
				LocalDateTime arrivalDateTime = LocalDateTime.parse(words[6], DATE_FORMATER);

				long minutes = ChronoUnit.MINUTES.between(departureDateTime, arrivalDateTime);
				long hours = ChronoUnit.HOURS.between(departureDateTime, arrivalDateTime);
				String duration = String.format(" %02d:%02d", hours, minutes%60);
				
				StringJoiner joiner = new StringJoiner(KafkaConstants.SEPARATOR);

				joiner.add(words[0]);
				joiner.add(words[1]);
				joiner.add(duration);
				return joiner.toString();
			}
		})
		.print(Printed.toSysOut());
		//.to(KafkaConstants.TOPIC_NAME + "-dur");

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
