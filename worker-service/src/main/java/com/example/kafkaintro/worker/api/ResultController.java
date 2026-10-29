package com.example.kafkaintro.worker.api;

import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaintro.worker.persistence.NumberResult;
import com.example.kafkaintro.worker.persistence.NumberResultRepository;
import com.example.kafkaintro.worker.persistence.WordResult;
import com.example.kafkaintro.worker.persistence.WordResultRepository;

@RestController
@RequestMapping("/api/results")
public class ResultController {

    private static final Sort NEWEST_FIRST = Sort.by(Sort.Direction.DESC, "processedAt");

    private final NumberResultRepository numberResults;
    private final WordResultRepository wordResults;

    public ResultController(NumberResultRepository numberResults, WordResultRepository wordResults) {
        this.numberResults = numberResults;
        this.wordResults = wordResults;
    }

    @GetMapping("/numbers")
    public List<NumberResult> numbers() {
        return numberResults.findAll(NEWEST_FIRST);
    }

    @GetMapping("/words")
    public List<WordResult> words() {
        return wordResults.findAll(NEWEST_FIRST);
    }
}
