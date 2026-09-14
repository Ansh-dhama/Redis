package org.example.Protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RespCommandReader {

    public List<String> read(
            BufferedReader reader
    ) throws IOException {

        String arrayHeader =
                reader.readLine();

        if (arrayHeader == null) {
            return null;
        }

        if (!arrayHeader.startsWith("*")) {
            throw new IllegalArgumentException(
                    "Expected RESP array"
            );
        }

        int argumentCount =
                Integer.parseInt(
                        arrayHeader.substring(1)
                );

        List<String> arguments =
                new ArrayList<>();

        for (int i = 0;
             i < argumentCount;
             i++) {

            String bulkHeader =
                    reader.readLine();

            if (bulkHeader == null
                    || !bulkHeader.startsWith("$")) {

                throw new IllegalArgumentException(
                        "Expected bulk string"
                );
            }

            int length =
                    Integer.parseInt(
                            bulkHeader.substring(1)
                    );

            String value =
                    reader.readLine();

            if (value == null) {
                throw new IllegalArgumentException(
                        "Missing bulk string"
                );
            }

            if (value.length() != length) {
                throw new IllegalArgumentException(
                        "Invalid bulk string length"
                );
            }

            arguments.add(value);
        }

        return arguments;
    }
}