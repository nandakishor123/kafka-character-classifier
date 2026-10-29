package com.example.kafkaintro.producer.api;

import java.util.UUID;

public record PublishResponse(UUID eventId, String topic, String key, int partition, long offset) {
}
