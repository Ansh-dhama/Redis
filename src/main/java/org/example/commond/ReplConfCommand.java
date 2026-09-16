package org.example.commond;

import org.example.Protocol.RespSerializer;

import java.util.List;

public class ReplConfCommand
        implements Command {

    private final RespSerializer serializer;


    public ReplConfCommand(
            RespSerializer serializer
    ) {
        this.serializer = serializer;
    }


    @Override
    public byte[] execute(
            List<String> args
    ) {

        System.out.println(
                "REPLCONF received: " + args
        );

        return serializer.simpleString(
                "OK"
        );
    }
}