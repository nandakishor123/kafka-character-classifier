package com.example.kafkaintro.producer.classify;

import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

@Component
public class InputClassifier {

    private static final Pattern NUMBER = Pattern.compile("-?\\d+");
    private static final Pattern WORD = Pattern.compile("[A-Za-z]+");

    public InputType classify(String input) {
        if (NUMBER.matcher(input).matches()) {
           return InputType.NUMBER;
        } else if (WORD.matcher(input).matches()) {
           return InputType.WORD;
        } else {
            throw new UnsupportedInputException("Unsupported input: " + input);
        }
    }
}
