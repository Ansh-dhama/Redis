package org.example.commond;

import org.example.Protocol.RespSerializer;
import org.example.commond.Command;
import org.example.storage.RedisStore;

import java.util.List;

public class GetCommand implements Command {

    private final RedisStore redisStore;

    private final RespSerializer serializer;


    public GetCommand(
            RedisStore redisStore,
            RespSerializer serializer
    ) {

        this.redisStore =
                redisStore;

        this.serializer =
                serializer;
    }


    @Override
    public byte[] execute(
            List<String> args
    ) {

        if (args.size() != 1) {

            return serializer.error(
                    "wrong number of arguments for GET"
            );
        }


        String key =
                args.get(0);


        String value =
                redisStore.get(key);


        if (value == null) {

            return serializer.nullBulkString();
        }


        return serializer.bulkString(
                value
        );
    }
}