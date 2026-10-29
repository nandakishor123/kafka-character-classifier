package com.example.kafkaintro.producer.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Spring's KafkaAdmin creates these on startup if they don't exist yet. */
@Configuration
public class TopicConfig {

    public static final String NUMBERS_TOPIC = "numbers";
    public static final String WORDS_TOPIC = "words";

    // 3 partitions => up to 3 worker instances in one group can consume in parallel.
    private static final int PARTITIONS = 3;

    @Bean
    NewTopic numbersTopic() {
        return TopicBuilder.name(NUMBERS_TOPIC).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic wordsTopic() {
        return TopicBuilder.name(WORDS_TOPIC).partitions(PARTITIONS).replicas(1).build();
    }
}
