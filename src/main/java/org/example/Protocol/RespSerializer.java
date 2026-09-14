package org.example.Protocol;

import java.nio.charset.StandardCharsets;

public class RespSerializer {

    public byte[] simpleString(String value) {

        String response =
                "+" + value + "\r\n";

        return response.getBytes(
                StandardCharsets.UTF_8
        );
    }

    public byte[] bulkString(String value) {

        byte[] valueBytes =
                value.getBytes(
                        StandardCharsets.UTF_8
                );

        String header =
                "$"
                + valueBytes.length
                + "\r\n";

        byte[] headerBytes =
                header.getBytes(
                        StandardCharsets.UTF_8
                );

        byte[] ending =
                "\r\n".getBytes(
                        StandardCharsets.UTF_8
                );

        byte[] response =
                new byte[
                        headerBytes.length
                        + valueBytes.length
                        + ending.length
                ];

        int position = 0;

        System.arraycopy(
                headerBytes,
                0,
                response,
                position,
                headerBytes.length
        );

        position += headerBytes.length;

        System.arraycopy(
                valueBytes,
                0,
                response,
                position,
                valueBytes.length
        );

        position += valueBytes.length;

        System.arraycopy(
                ending,
                0,
                response,
                position,
                ending.length
        );

        return response;
    }

    public byte[] nullBulkString() {

        return "$-1\r\n".getBytes(
                StandardCharsets.UTF_8
        );
    }

    public byte[] error(String message) {

        String response =
                "-ERR "
                + message
                + "\r\n";

        return response.getBytes(
                StandardCharsets.UTF_8
        );
    }
}