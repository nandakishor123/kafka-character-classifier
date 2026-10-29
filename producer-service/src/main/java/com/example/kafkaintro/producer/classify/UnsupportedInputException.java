package com.example.kafkaintro.producer.classify;

public class UnsupportedInputException extends RuntimeException {

    public UnsupportedInputException(String input) {
        super("Input must be an integer or a word made of letters A-Z, got: '" + input + "'");
    }
}
