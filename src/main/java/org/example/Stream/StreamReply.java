package org.example.Stream;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class StreamReply {

    private static void write(
            ByteArrayOutputStream out,
            String text
    ) {
        out.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    }

    private static void bulk(
            ByteArrayOutputStream out,
            String value
    ) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        write(out, "$" + bytes.length + "\r\n");
        out.writeBytes(bytes);
        write(out, "\r\n");
    }

    private static void entry(
            ByteArrayOutputStream out,
            StreamEntry entry
    ) {

        // [entry-id, [field1, value1, ...]]
        write(out, "*2\r\n");
        bulk(out, entry.id());

        Map<String, String> fields = entry.fields();

        write(out, "*" + (fields.size() * 2) + "\r\n");

        for (var field : fields.entrySet()) {
            bulk(out, field.getKey());
            bulk(out, field.getValue());
        }
    }

    // Response for XRANGE
    public static byte[] range(List<StreamEntry> entries) {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        write(out, "*" + entries.size() + "\r\n");

        for (StreamEntry entry : entries) {
            entry(out, entry);
        }

        return out.toByteArray();
    }

    // Response for XREAD
    public static byte[] read(
            String streamName,
            List<StreamEntry> entries
    ) {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // [[stream-name, [entries...]]]
        write(out, "*1\r\n*2\r\n");
        bulk(out, streamName);

        out.writeBytes(range(entries));

        return out.toByteArray();
    }

    public static byte[] nullReply() {
        return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
    }
}