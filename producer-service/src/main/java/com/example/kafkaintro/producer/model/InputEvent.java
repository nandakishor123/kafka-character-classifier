package com.example.kafkaintro.producer.model;

import java.time.Instant;
import java.util.UUID;

/** The message published to Kafka. Its JSON shape is the contract with the worker. */
public record InputEvent(UUID eventId, String value, Instant receivedAt) {

    public static InputEvent of(String value) {
        return new InputEvent(UUID.randomUUID(), value, Instant.now());
    }
}
