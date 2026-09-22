package org.example.Stream;

import java.util.Map;

public record StreamEntry(
        String id,
        Map<String, String> fields
) {
}