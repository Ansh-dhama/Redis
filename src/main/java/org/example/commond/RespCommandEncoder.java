package org.example.commond;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class RespCommandEncoder {

    private RespCommandEncoder() {
    }


    public static byte[] encode(
            List<String> parts
    ) {

        StringBuilder command =
                new StringBuilder();


        command.append("*")
                .append(parts.size())
                .append("\r\n");


        for (String part : parts) {

            byte[] bytes =
                    part.getBytes(
                            StandardCharsets.UTF_8
                    );


            command.append("$")
                    .append(bytes.length)
                    .append("\r\n");

            command.append(part)
                    .append("\r\n");
        }


        return command
                .toString()
                .getBytes(
                        StandardCharsets.UTF_8
                );
    }


    public static byte[] encode(
            String... parts
    ) {

        return encode(
                List.of(parts)
        );
    }
}