package org.processor.util;

public class KeyBuilder {
    public static String of(String owner, String executionId) {
        return owner + "#" + executionId;
    }
}