package com.example.kafka_streams_examples.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.example.kafka_streams_examples.util.Pair;

public class PairSerdes implements Serde<Pair> {

	public Serializer<Pair> serializer() {
		return new PairSerializerDeserializer();
	}

	public Deserializer<Pair> deserializer() {
		return new PairSerializerDeserializer();
	}

}
