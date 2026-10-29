package com.example.kafkaintro.worker.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface WordResultRepository extends JpaRepository<WordResult, Long> {

    boolean existsByKafkaTopicAndKafkaPartitionAndKafkaOffset(String topic, int partition, long offset);
}
