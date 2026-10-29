package com.example.kafkaintro.producer.classify;

import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

@Component
public class InputClassifier {

    private static final Pattern NUMBER = Pattern.compile("-?\\d+");
    private static final Pattern WORD = Pattern.compile("[A-Za-z]+");

    public InputType classify(String input) {
        InputType type = null;
        try {
            if (NUMBER.matcher(input).matches()) {
                type = InputType.NUMBER;
            } else if (WORD.matcher(input).matches()) {
                type = InputType.WORD;
            }
        } catch (Exception e) {
            throw new UnsupportedInputException(input);
        }
        return type;
    }
}
