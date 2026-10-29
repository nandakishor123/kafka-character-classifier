package com.example.kafkaintro.worker.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.example.kafkaintro.worker.analysis.NumberAnalyzer;
import com.example.kafkaintro.worker.analysis.Parity;
import com.example.kafkaintro.worker.analysis.WordAnalyzer;

/**
 * Each listener calls {@code ack.acknowledge()} only once the record has been processed.
 * If processing throws, there is no ack, and the error handler retries the same offset
 * (see {@link KafkaErrorConfig}).
 */
@Component
public class InputListeners {

    private static final Logger log = LoggerFactory.getLogger(InputListeners.class);

    private final NumberAnalyzer numberAnalyzer;
    private final WordAnalyzer wordAnalyzer;

    public InputListeners(NumberAnalyzer numberAnalyzer, WordAnalyzer wordAnalyzer) {
        this.numberAnalyzer = numberAnalyzer;
        this.wordAnalyzer = wordAnalyzer;
    }

    @KafkaListener(topics = "numbers")
    public void onNumber(ConsumerRecord<String, InputEvent> record, Acknowledgment ack) {
        log.info("Received number key={} from {}-{}@{}",
                record.key(), record.topic(), record.partition(), record.offset());
        Parity parity = numberAnalyzer.parity(record.value().value());
        log.info("{} is {}", record.value().value(), parity);
        ack.acknowledge();
    }

    @KafkaListener(topics = "words")
    public void onWord(ConsumerRecord<String, InputEvent> record, Acknowledgment ack) {
        log.info("Received word key={} from {}-{}@{}",
                record.key(), record.topic(), record.partition(), record.offset());
        WordAnalyzer.LetterCounts counts = wordAnalyzer.count(record.value().value());
        log.info("'{}' vowels={} consonants={}", record.value().value(), counts.vowels(), counts.consonants());
        ack.acknowledge();
    }
}
