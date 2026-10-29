package com.example.kafkaintro.worker.kafka;

import java.time.Instant;
import java.util.UUID;

/**
 * The worker's own view of the message on the numbers/words topics. Deliberately not shared
 * with the producer: the services only agree on the JSON shape.
 */
public record InputEvent(UUID eventId, String value, Instant receivedAt) {
}
