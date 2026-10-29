package com.example.kafkaintro.worker.persistence;

import java.time.Instant;
import java.util.UUID;

import com.example.kafkaintro.worker.analysis.Parity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

// "value", "partition" and "offset" are SQL keywords, hence the prefixed column names.
@Entity
@Table(name = "number_result", uniqueConstraints = @UniqueConstraint(
        name = "uk_number_result_source", columnNames = {"kafka_topic", "kafka_partition", "kafka_offset"}))
public class NumberResult {

    @Id
    @GeneratedValue
    private Long id;

    private UUID eventId;

    @Column(name = "number_value", nullable = false)
    private String numberValue;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Parity parity;

    @Column(name = "kafka_topic", nullable = false)
    private String kafkaTopic;

    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    private Instant receivedAt;

    private Instant processedAt;

    protected NumberResult() {
    }

    public NumberResult(UUID eventId, String numberValue, Parity parity, String kafkaTopic,
            int kafkaPartition, long kafkaOffset, Instant receivedAt, Instant processedAt) {
        this.eventId = eventId;
        this.numberValue = numberValue;
        this.parity = parity;
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

    public String getNumberValue() {
        return numberValue;
    }

    public Parity getParity() {
        return parity;
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
