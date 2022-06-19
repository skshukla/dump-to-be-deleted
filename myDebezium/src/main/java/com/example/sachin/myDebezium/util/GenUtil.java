package com.example.sachin.myDebezium.util;

import java.util.Random;
import java.util.stream.IntStream;

public class GenUtil {

    public static final String getRandomStringOfLength(int n) {
        final StringBuilder sb = new StringBuilder();
        IntStream.range(0, n).forEach(i -> sb.append(getRandomChar()));
        return sb.toString();
    }

    public static final String getRandomChar() {
        final boolean isUpperCase = new Random().nextBoolean();
        int n =  new Random().nextInt(26);
        final char ch = isUpperCase ? (char)('A' + n) : (char)('a' + n);
        return String.valueOf(ch);
    }
}
