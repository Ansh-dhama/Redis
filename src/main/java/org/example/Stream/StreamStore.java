package org.example.Stream;

import org.example.Stream.StreamEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {

    private final Map<String, Stream> streams =
            new ConcurrentHashMap<>();

    // XADD: Add a new entry
    // Existing XADD calls can continue using this method.
    public String add(String key, Map<String, String> fields) {
        return add(key, "*", fields);
    }

    // Supports automatic IDs and explicit IDs.
// Explicit IDs are needed when replaying saved or replicated entries.
    public String add(
            String key,
            String requestedId,
            Map<String, String> fields
    ) {
        Stream stream = streams.computeIfAbsent(
                key, k -> new Stream()
        );

        synchronized (stream) {

            long millis;
            long sequence;

            if (requestedId.equals("*")) {

                millis = Math.max(
                        System.currentTimeMillis(),
                        stream.lastMillis
                );

                sequence = millis == stream.lastMillis
                        ? stream.sequence + 1
                        : 0;

            } else {

                String[] parts = requestedId.split("-", -1);

                if (parts.length != 2) {
                    throw new IllegalArgumentException("invalid stream ID");
                }

                try {
                    millis = Long.parseLong(parts[0]);
                    sequence = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("invalid stream ID");
                }

                if (millis < 0 || sequence < 0 ||
                        (millis == 0 && sequence == 0)) {
                    throw new IllegalArgumentException("invalid stream ID");
                }

                if (millis < stream.lastMillis ||
                        (millis == stream.lastMillis
                                && sequence <= stream.sequence)) {

                    throw new IllegalArgumentException(
                            "stream ID must be greater than previous ID"
                    );
                }
            }

            String id = millis + "-" + sequence;

            stream.lastMillis = millis;
            stream.sequence = sequence;

            stream.entries.add(
                    new StreamEntry(
                            id,
                            new LinkedHashMap<>(fields)
                    )
            );

            stream.notifyAll();

            return id;
        }
    }

    // Create replayable XADD commands for ALL stored stream entries.
    public List<List<String>> snapshotCommands() {

        List<List<String>> result = new ArrayList<>();

        for (var streamItem : streams.entrySet()) {

            String streamName = streamItem.getKey();
            Stream stream = streamItem.getValue();

            synchronized (stream) {

                for (StreamEntry entry : stream.entries) {

                    List<String> command = new ArrayList<>();

                    command.add("XADD");
                    command.add(streamName);
                    command.add(entry.id());

                    for (var field : entry.fields().entrySet()) {
                        command.add(field.getKey());
                        command.add(field.getValue());
                    }

                    result.add(command);
                }
            }
        }

        return result;
    }

    public void clear() {
        streams.clear();
    }
    // XRANGE: Read entries between two IDs
    public List<StreamEntry> range(
            String key,
            String start,
            String end
    ) {

        Stream stream = streams.get(key);

        if (stream == null) {
            return List.of();
        }

        synchronized (stream) {

            List<StreamEntry> result = new ArrayList<>();

            for (StreamEntry entry : stream.entries) {

                if (compare(entry.id(), start) >= 0
                        && compare(entry.id(), end) <= 0) {

                    result.add(entry);
                }
            }

            return result;
        }
    }

    // XREAD: Read entries newer than the given ID
    // BLOCK: Wait until a new entry arrives
    public List<StreamEntry> readAfter(
            String key,
            String lastId,
            long blockMs
    ) throws InterruptedException {

        Stream stream = streams.computeIfAbsent(
                key, k -> new Stream()
        );

        synchronized (stream) {

            // $ means: start after the current latest entry
            if (lastId.equals("$")) {

                lastId = stream.entries.isEmpty()
                        ? "0-0"
                        : stream.entries.get(
                        stream.entries.size() - 1
                ).id();
            }

            long deadline =
                    System.currentTimeMillis() + blockMs;

            while (true) {

                List<StreamEntry> result =
                        new ArrayList<>();

                for (StreamEntry entry : stream.entries) {

                    if (compare(entry.id(), lastId) > 0) {
                        result.add(entry);
                    }
                }

                if (!result.isEmpty()) {
                    return result;
                }

                // -1 means: non-blocking read
                if (blockMs == -1) {
                    return List.of();
                }

                // 0 means: wait indefinitely
                if (blockMs == 0) {

                    stream.wait();

                } else {

                    long remaining =
                            deadline - System.currentTimeMillis();

                    if (remaining <= 0) {
                        return List.of();
                    }

                    stream.wait(remaining);
                }
            }
        }
    }

    // Compare stream IDs numerically
    private int compare(String first, String second) {

        if (second.equals("-")) return 1;
        if (second.equals("+")) return -1;

        String[] a = first.split("-");
        String[] b = second.split("-");

        long aMillis = Long.parseLong(a[0]);
        long bMillis = Long.parseLong(b[0]);

        int timeComparison =
                Long.compare(aMillis, bMillis);

        if (timeComparison != 0) {
            return timeComparison;
        }

        long aSequence = Long.parseLong(a[1]);
        long bSequence = b.length == 1
                ? 0
                : Long.parseLong(b[1]);

        return Long.compare(aSequence, bSequence);
    }

    private static class Stream {

        long lastMillis = 0;
        long sequence = 0;

        final List<StreamEntry> entries =
                new ArrayList<>();
    }
}