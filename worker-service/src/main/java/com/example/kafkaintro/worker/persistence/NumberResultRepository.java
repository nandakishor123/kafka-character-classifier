package com.example.kafkaintro.worker.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface NumberResultRepository extends JpaRepository<NumberResult, Long> {

    boolean existsByKafkaTopicAndKafkaPartitionAndKafkaOffset(String topic, int partition, long offset);
}
