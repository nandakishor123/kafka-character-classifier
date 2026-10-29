package com.example.kafkaintro.worker.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Where a record came from. Unique per record, so it doubles as an idempotency key. */
public record RecordSource(String topic, int partition, long offset) {

    public static RecordSource of(ConsumerRecord<?, ?> record) {
        return new RecordSource(record.topic(), record.partition(), record.offset());
    }
}
