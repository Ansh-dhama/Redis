package org.example.commond;


import org.example.Protocol.RespSerializer;

import java.util.List;

public class EchoCommand implements Command {

    private final RespSerializer serializer;

    public EchoCommand(
            RespSerializer serializer
    ) {
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(
            List<String> args
    ) {

        if (args.size() != 1) {

            return serializer.error(
                    "wrong number of arguments for ECHO"
            );
        }

        return serializer.bulkString(
                args.get(0)
        );
    }
}