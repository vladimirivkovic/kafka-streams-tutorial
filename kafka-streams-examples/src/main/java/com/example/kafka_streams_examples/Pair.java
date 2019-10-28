package com.example.kafka_streams_examples;

public class Pair {
	public Long cnt;
	public Long sum;

	Pair() {
		cnt = 0L;
		sum = 0L;
	}

	Pair(Long cnt, Long sum) {
		this.cnt = cnt;
		this.sum = sum;
	}
}
