package com.example.kafkaintro.producer.classify;

import com.example.kafkaintro.producer.kafka.TopicConfig;

public enum InputType {
    NUMBER(TopicConfig.NUMBERS_TOPIC),
    WORD(TopicConfig.WORDS_TOPIC);

    private final String topic;

    InputType(String topic) {
        this.topic = topic;
    }

    public String topic() {
        return topic;
    }
}
