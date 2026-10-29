package com.example.kafkaintro.producer.api;

import jakarta.validation.constraints.NotBlank;

public record InputRequest(@NotBlank String value) {
}
