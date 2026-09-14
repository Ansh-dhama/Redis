package org.example.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    public List<String> parse(InputStream input)
            throws IOException {
         int firstByte = input.read();
         if (firstByte == -1) {
             return null;
         }

        if (firstByte != '*') {
            throw new IllegalArgumentException(
                    "Expected RESP Array"
            );
        }

        int numberOfElements = Integer.parseInt(readLine(input));
        List<String> result = new ArrayList<>(numberOfElements);

        for (int i = 0; i < numberOfElements; i++) {
            int type = input.read();

            if (type != '$') {
                throw new IllegalArgumentException(
                        "Expected RESP Bulk String"
                );
            }
            int length = Integer.parseInt(readLine(input));
            byte[] data = input.readNBytes(length);

            if (data.length != length) {
                throw new IOException(
                        "Unexpected end of stream"
                );
            }

            exceptCRLF(input);
            result.add(
                    new String(
                            data,
                            StandardCharsets.UTF_8
                    )
            );
        }
        return result;
    }

    private void exceptCRLF(InputStream input) throws IOException {
        int first = input.read();
        int second = input.read();

        if (first != '\r' || second != '\n') {
            throw new IllegalArgumentException(
                    "Expected CRLF"
            );
        }

    }

    private String readLine(InputStream input) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();

        while (true) {
            int current = input.read();
            if (current == -1) {
                throw new IOException(
                        "Unexpected end of stream"
                );
            }

            if (current == '\r') {

                int next = input.read();

                if (next != '\n') {
                    throw new IllegalArgumentException(
                            "Expected LF after CR"
                    );
                }

                break;
            }
            stringBuilder.append((char) current);
        }
        return stringBuilder.toString();
    }
}