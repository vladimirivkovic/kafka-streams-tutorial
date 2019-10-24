package com.example.kafka_streams_examples;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

public class FlightNumberCounterExample {
	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("flight-number-counter");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		flights
		.map(new KeyValueMapper<Long, String, KeyValue<String, Double>>() {
			@Override
			public KeyValue<String, Double> apply(Long key, String value) {
				String[] words = value.split(KafkaConstants.SEPARATOR);
				return new KeyValue<>(words[0], Double.parseDouble(words[9]));
			}
		})
		.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
		.count()
		.toStream()
		.to(KafkaConstants.TOPIC_NAME + "-cnt", Produced.with(Serdes.String(), Serdes.Long()));

		@SuppressWarnings("resource")
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
