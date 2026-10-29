package com.example.kafkaintro.producer.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaintro.producer.classify.InputClassifier;
import com.example.kafkaintro.producer.classify.InputType;
import com.example.kafkaintro.producer.classify.UnsupportedInputException;
import com.example.kafkaintro.producer.kafka.InputPublisher;
import com.example.kafkaintro.producer.model.InputEvent;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/messages")
public class InputController {

    private final InputClassifier classifier;
    private final InputPublisher publisher;

    public InputController(InputClassifier classifier, InputPublisher publisher) {
        this.classifier = classifier;
        this.publisher = publisher;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse submit(@Valid @RequestBody InputRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        String value = request.value().trim();
        InputType type = classifier.classify(value);
        InputEvent event = InputEvent.of(value);
        RecordMetadata metadata = publisher.publish(type, event);
        return new PublishResponse(event.eventId(), metadata.topic(), value,
                metadata.partition(), metadata.offset());
    }

    @ExceptionHandler(UnsupportedInputException.class)
    ProblemDetail handleUnsupported(UnsupportedInputException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
