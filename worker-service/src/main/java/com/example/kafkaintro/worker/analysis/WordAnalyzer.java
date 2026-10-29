package com.example.kafkaintro.worker.analysis;

import org.springframework.stereotype.Component;

@Component
public class WordAnalyzer {

    private static final String VOWELS = "aeiou";

    public record LetterCounts(int vowels, int consonants) {
    }

    /** Vowels are a, e, i, o, u (any case). Every other ASCII letter, including y, is a consonant. */
    public LetterCounts count(String word) {
        if (word == null || !word.matches("[A-Za-z]+")) {
            throw new IllegalArgumentException("Not a word: '" + word + "'");
        }
        int vowels = 0;
        for (char c : word.toLowerCase().toCharArray()) {
            if (VOWELS.indexOf(c) >= 0) {
                vowels++;
            }
        }
        return new LetterCounts(vowels, word.length() - vowels);
    }
}
