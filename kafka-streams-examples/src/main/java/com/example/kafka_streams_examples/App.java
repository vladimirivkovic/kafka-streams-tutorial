package com.example.kafka_streams_examples;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

public class App {
	public static void main(String[] args) {
		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique in the
		// Kafka cluster against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.TOPIC_NAME + "-streams-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConstants.TOPIC_NAME + "-streams-example-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
		// Specify default (de)serializers for record keys and for record values.
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		flights.map(new KeyValueMapper<Long, String, KeyValue<String, Double>>() {
			@Override
			public KeyValue<String, Double> apply(Long key, String value) {
				String[] words = value.split("#");
				return new KeyValue<>(words[0], Double.parseDouble(words[9]));
			}
		})
		.groupByKey(Grouped.with(
			      Serdes.String(),
			      Serdes.Double()))
		.count().toStream()
		.to("pek-sha-cnt", Produced.with(Serdes.String(), Serdes.Long()));
		//.foreach((k, v) -> System.out.println(k + " " + v));

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
