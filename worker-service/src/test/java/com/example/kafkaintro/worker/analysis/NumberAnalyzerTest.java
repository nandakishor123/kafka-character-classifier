package com.example.kafkaintro.worker.analysis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class NumberAnalyzerTest {

    private final NumberAnalyzer analyzer = new NumberAnalyzer();

    @ParameterizedTest
    @CsvSource({
            "0, EVEN",
            "42, EVEN",
            "7, ODD",
            "-3, ODD",
            "-10, EVEN",
            "123456789012345678901234567891, ODD"
    })
    void detectsParity(String number, Parity expected) {
        assertThat(analyzer.parity(number)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "abc", "1.5", "-"})
    void rejectsNonIntegers(String input) {
        assertThatThrownBy(() -> analyzer.parity(input)).isInstanceOf(IllegalArgumentException.class);
    }
}
