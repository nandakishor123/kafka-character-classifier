package com.example.kafkaintro.producer.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.kafkaintro.producer.classify.InputType;
import com.example.kafkaintro.producer.model.InputEvent;

@Component
public class InputPublisher {

    private static final Logger log = LoggerFactory.getLogger(InputPublisher.class);

    private final KafkaTemplate<String, InputEvent> kafkaTemplate;

    public InputPublisher(KafkaTemplate<String, InputEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes the event keyed by its value, so the same value always hashes to the same
     * partition. Waits for the broker ack so the caller learns where the record landed.
     */
    public RecordMetadata publish(InputType type, InputEvent event)
            throws ExecutionException, InterruptedException, TimeoutException {
        RecordMetadata metadata = kafkaTemplate.send(type.topic(), event.value(), event)
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata();
        log.info("Published key={} to {}-{}@{}",
                event.value(), metadata.topic(), metadata.partition(), metadata.offset());
        return metadata;
    }
}
