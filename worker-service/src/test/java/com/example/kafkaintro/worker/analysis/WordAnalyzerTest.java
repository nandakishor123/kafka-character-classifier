package com.example.kafkaintro.worker.analysis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class WordAnalyzerTest {

    private final WordAnalyzer analyzer = new WordAnalyzer();

    @ParameterizedTest
    @CsvSource({
            "apple, 2, 3",
            "Kafka, 2, 3",
            "AEIOU, 5, 0",
            "rhythm, 0, 6",
            "a, 1, 0"
    })
    void countsVowelsAndConsonants(String word, int vowels, int consonants) {
        assertThat(analyzer.count(word)).isEqualTo(new WordAnalyzer.LetterCounts(vowels, consonants));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "he!!o", "abc1", "two words"})
    void rejectsNonWords(String input) {
        assertThatThrownBy(() -> analyzer.count(input)).isInstanceOf(IllegalArgumentException.class);
    }
}
