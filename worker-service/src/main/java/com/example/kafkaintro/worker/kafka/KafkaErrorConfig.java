package com.example.kafkaintro.worker.kafka;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Failed records are retried, then parked on {@code <topic>.DLT} so the partition can move on.
 * Spring Boot plugs the {@link DefaultErrorHandler} bean into the listener container.
 */
@Configuration
public class KafkaErrorConfig {

    // The recoverer publishes to the same partition number on the DLT, so match the source topics.
    private static final int PARTITIONS = 3;

    @Bean
    NewTopic numbersDeadLetterTopic() {
        return TopicBuilder.name("numbers.DLT").partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic wordsDeadLetterTopic() {
        return TopicBuilder.name("words.DLT").partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> deadLetterTemplate) {
        // Name the DLT explicitly: the library default suffix differs between Spring Kafka versions.
        var recoverer = new DeadLetterPublishingRecoverer(deadLetterTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        // 1 attempt + 2 retries, 1s apart. Then the record goes to the DLT.
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        // With manual acks, a recovered record's offset is not committed unless we ask for it.
        handler.setCommitRecovered(true);
        // Bad input will never succeed, so skip the retries (deserialization errors already are).
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    /**
     * Records that failed to deserialize reach the recoverer as raw bytes, while records that
     * failed in the listener are already InputEvent objects, so serialize each kind as-is.
     */
    @Bean
    KafkaTemplate<String, Object> deadLetterTemplate(KafkaProperties properties) {
        Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(Object.class, new JsonSerializer<>().noTypeInfo());

        Map<String, Object> config = properties.buildProducerProperties(null);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, Object>(config,
                new StringSerializer(), new DelegatingByTypeSerializer(serializers, true));
        return new KafkaTemplate<>(factory);
    }
}
