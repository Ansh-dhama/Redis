package org.example.commond;

import org.example.Protocol.RespSerializer;
import org.example.persistence.RdbWriter;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class SaveCommand
        implements Command {

    private final RedisStore store;

    private final RdbWriter writer;

    private final Path path;

    private final RespSerializer serializer;


    public SaveCommand(
            RedisStore store,
            RdbWriter writer,
            Path path,
            RespSerializer serializer
    ) {

        this.store = store;

        this.writer = writer;

        this.path = path;

        this.serializer =
                serializer;
    }


    @Override
    public byte[] execute(
            List<String> args
    ) {

        if (!args.isEmpty()) {

            return serializer.error(
                    "wrong number of arguments for SAVE"
            );
        }


        try {

            writer.save(
                    path,
                    store
            );


            return serializer.simpleString(
                    "OK"
            );

        } catch (IOException e) {

            return serializer.error(
                    "could not save RDB: "
                            + e.getMessage()
            );
        }
    }
}