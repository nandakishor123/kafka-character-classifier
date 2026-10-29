package com.example.kafkaintro.worker.analysis;

import org.springframework.stereotype.Component;

@Component
public class NumberAnalyzer {

    /** Uses only the last digit, so numbers of any size work without parsing. */
    public Parity parity(String number) {
        if (number == null || !number.matches("-?\\d+")) {
            throw new IllegalArgumentException("Not an integer: '" + number + "'");
        }
        int lastDigit = number.charAt(number.length() - 1) - '0';
        return lastDigit % 2 == 0 ? Parity.EVEN : Parity.ODD;
    }
}
