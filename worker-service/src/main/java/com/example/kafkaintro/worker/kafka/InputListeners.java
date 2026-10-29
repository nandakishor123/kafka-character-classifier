package com.example.kafkaintro.worker.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.example.kafkaintro.worker.service.ResultService;

/**
 * Commit-after-write: each listener calls {@code ack.acknowledge()} only once the result is
 * stored. If storing throws, there is no ack, and the error handler retries the same offset
 * (see {@link KafkaErrorConfig}).
 */
@Component
public class InputListeners {

    private static final Logger log = LoggerFactory.getLogger(InputListeners.class);

    private final ResultService resultService;

    public InputListeners(ResultService resultService) {
        this.resultService = resultService;
    }

    @KafkaListener(topics = "numbers")
    public void onNumber(ConsumerRecord<String, InputEvent> record, Acknowledgment ack) {
        log.info("Received number key={} from {}-{}@{}",
                record.key(), record.topic(), record.partition(), record.offset());
        resultService.saveNumber(record.value(), RecordSource.of(record));
        ack.acknowledge();
    }

    @KafkaListener(topics = "words")
    public void onWord(ConsumerRecord<String, InputEvent> record, Acknowledgment ack) {
        log.info("Received word key={} from {}-{}@{}",
                record.key(), record.topic(), record.partition(), record.offset());
        resultService.saveWord(record.value(), RecordSource.of(record));
        ack.acknowledge();
    }
}
