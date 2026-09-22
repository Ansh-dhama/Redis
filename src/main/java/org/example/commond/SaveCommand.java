package org.example.commond;

import org.example.Protocol.RespSerializer;
import org.example.Stream.StreamSnapshotFile;
import org.example.Stream.StreamStore;
import org.example.persistence.RdbWriter;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class SaveCommand implements Command {

    private final RedisStore redisStore;
    private final StreamStore streamStore;
    private final Path path;
    private final RespSerializer serializer;

    public SaveCommand(
            RedisStore redisStore,
            StreamStore streamStore,
            Path path,
            RespSerializer serializer
    ) {
        this.redisStore = redisStore;
        this.streamStore = streamStore;
        this.path = path;
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(List<String> args) {

        if (!args.isEmpty()) {
            return serializer.error(
                    "wrong number of arguments for SAVE"
            );
        }

        try {

            new RdbWriter().save(path, redisStore);

            new StreamSnapshotFile().save(
                    path,
                    streamStore
            );

            return serializer.simpleString("OK");

        } catch (IOException e) {

            return serializer.error(
                    "SAVE failed: " + e.getMessage()
            );
        }
    }
}