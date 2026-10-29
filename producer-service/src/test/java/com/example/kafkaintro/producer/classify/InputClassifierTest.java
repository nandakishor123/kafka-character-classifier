package com.example.kafkaintro.producer.classify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InputClassifierTest {

    private final InputClassifier classifier = new InputClassifier();

    @ParameterizedTest
    @ValueSource(strings = {"0", "42", "-7", "123456789012345678901234567890"})
    void classifiesIntegersAsNumbers(String input) {
        assertThat(classifier.classify(input)).isEqualTo(InputType.NUMBER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"apple", "Kafka", "a", "RHYTHM"})
    void classifiesLettersAsWords(String input) {
        assertThat(classifier.classify(input)).isEqualTo(InputType.WORD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"he!!o", "3.14", "abc123", "two words", "-", ""})
    void rejectsEverythingElse(String input) {
        assertThatThrownBy(() -> classifier.classify(input))
                .isInstanceOf(UnsupportedInputException.class);
    }
}
