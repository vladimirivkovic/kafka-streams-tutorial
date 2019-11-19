package com.example.kafka_streams_examples;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;

import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;

public class PerDayCounterExample {
	public static void main(String[] args) {
		final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("per-day-counter");

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

		flights
		.map(new KeyValueMapper<Long, String, KeyValue<String, String>>() {
			@Override
			public KeyValue<String, String> apply(Long key, String value) {
				String[] words = value.split(KafkaConstants.SEPARATOR);
				String date = words[5].substring(0, 10);
				return new KeyValue<>(date, value);
			}
		})
		.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
		.count()
		.toStream()
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
