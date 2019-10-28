package com.example.kafka_streams_example.serdes;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.example.kafka_streams_examples.Pair;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PairSerializerDeserializer implements Serializer<Pair>, Deserializer<Pair> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	public PairSerializerDeserializer() {
	}

	@Override
	public void configure(Map<String, ?> props, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, Pair data) {
		if (data == null)
			return null;

		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
	}

	@Override
	public Pair deserialize(String topic, byte[] bytes) {
		if (bytes == null)
			return null;

		Pair data;
		try {
			data = objectMapper.readValue(bytes, Pair.class);
		} catch (Exception e) {
			throw new SerializationException(e);
		}

		return data;
	}

	@Override
	public void close() {
	}
}