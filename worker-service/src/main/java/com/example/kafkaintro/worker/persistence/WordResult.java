package com.example.kafkaintro.worker.persistence;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "word_result", uniqueConstraints = @UniqueConstraint(
        name = "uk_word_result_source", columnNames = {"kafka_topic", "kafka_partition", "kafka_offset"}))
public class WordResult {

    @Id
    @GeneratedValue
    private Long id;

    private UUID eventId;

    @Column(nullable = false)
    private String word;

    private int vowelCount;

    private int consonantCount;

    @Column(name = "kafka_topic", nullable = false)
    private String kafkaTopic;

    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    private Instant receivedAt;

    private Instant processedAt;

    protected WordResult() {
    }

    public WordResult(UUID eventId, String word, int vowelCount, int consonantCount, String kafkaTopic,
            int kafkaPartition, long kafkaOffset, Instant receivedAt, Instant processedAt) {
        this.eventId = eventId;
        this.word = word;
        this.vowelCount = vowelCount;
        this.consonantCount = consonantCount;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
        this.receivedAt = receivedAt;
        this.processedAt = processedAt;
    }

    public Long getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getWord() {
        return word;
    }

    public int getVowelCount() {
        return vowelCount;
    }

    public int getConsonantCount() {
        return consonantCount;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
