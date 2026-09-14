package org.example.commond;


import org.example.Protocol.RespSerializer;

import java.util.List;

public class PingCommand implements Command {

    private final RespSerializer serializer;

    public PingCommand(
            RespSerializer serializer
    ) {
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(
            List<String> args
    ) {

        return serializer.simpleString(
                "PONG"
        );
    }
}