package com.example.kafka_streams_examples.util;

public class Pair {
	public Long cnt;
	public Long sum;

	public Pair() {
		cnt = 0L;
		sum = 0L;
	}

	Pair(Long cnt, Long sum) {
		this.cnt = cnt;
		this.sum = sum;
	}
}
