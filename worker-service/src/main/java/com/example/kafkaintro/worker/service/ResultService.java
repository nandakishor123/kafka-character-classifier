package com.example.kafkaintro.worker.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.example.kafkaintro.worker.analysis.NumberAnalyzer;
import com.example.kafkaintro.worker.analysis.Parity;
import com.example.kafkaintro.worker.analysis.WordAnalyzer;
import com.example.kafkaintro.worker.kafka.InputEvent;
import com.example.kafkaintro.worker.kafka.RecordSource;
import com.example.kafkaintro.worker.persistence.NumberResult;
import com.example.kafkaintro.worker.persistence.NumberResultRepository;
import com.example.kafkaintro.worker.persistence.WordResult;
import com.example.kafkaintro.worker.persistence.WordResultRepository;

/**
 * Each save is a single {@code saveAndFlush}, which runs and commits in its own transaction,
 * so when these methods return the row is durably in the DB and the offset can be committed.
 *
 * <p>Delivery is at-least-once: if the worker dies after the DB commit but before the offset
 * commit, the record is redelivered. The unique (topic, partition, offset) constraint turns that
 * redelivery into a no-op instead of a duplicate row.
 */
@Service
public class ResultService {

    private static final Logger log = LoggerFactory.getLogger(ResultService.class);

    private final NumberAnalyzer numberAnalyzer;
    private final WordAnalyzer wordAnalyzer;
    private final NumberResultRepository numberResults;
    private final WordResultRepository wordResults;

    public ResultService(NumberAnalyzer numberAnalyzer, WordAnalyzer wordAnalyzer,
            NumberResultRepository numberResults, WordResultRepository wordResults) {
        this.numberAnalyzer = numberAnalyzer;
        this.wordAnalyzer = wordAnalyzer;
        this.numberResults = numberResults;
        this.wordResults = wordResults;
    }

    public void saveNumber(InputEvent event, RecordSource source) {
        Parity parity = numberAnalyzer.parity(event.value());
        var result = new NumberResult(event.eventId(), event.value(), parity, source.topic(),
                source.partition(), source.offset(), event.receivedAt(), Instant.now());
        try {
            numberResults.saveAndFlush(result);
            log.info("Stored {} is {}", event.value(), parity);
        } catch (DataIntegrityViolationException e) {
            if (!numberResults.existsByKafkaTopicAndKafkaPartitionAndKafkaOffset(
                    source.topic(), source.partition(), source.offset())) {
                throw e;
            }
            log.warn("Skipping redelivered record {}", source);
        }
    }

    public void saveWord(InputEvent event, RecordSource source) {
        WordAnalyzer.LetterCounts counts = wordAnalyzer.count(event.value());
        var result = new WordResult(event.eventId(), event.value(), counts.vowels(), counts.consonants(),
                source.topic(), source.partition(), source.offset(), event.receivedAt(), Instant.now());
        try {
            wordResults.saveAndFlush(result);
            log.info("Stored '{}' vowels={} consonants={}", event.value(), counts.vowels(), counts.consonants());
        } catch (DataIntegrityViolationException e) {
            if (!wordResults.existsByKafkaTopicAndKafkaPartitionAndKafkaOffset(
                    source.topic(), source.partition(), source.offset())) {
                throw e;
            }
            log.warn("Skipping redelivered record {}", source);
        }
    }
}
