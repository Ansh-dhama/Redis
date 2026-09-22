package org.example.commond;

import org.example.Protocol.RespSerializer;
import org.example.Stream.StreamStore;
import org.example.commond.Command;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class XAddCommand implements Command {

    private final StreamStore store;
    private final RespSerializer serializer;

    public XAddCommand(
            StreamStore store,
            RespSerializer serializer
    ) {
        this.store = store;
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(List<String> args) {

        if (args.size() < 4 || args.size() % 2 != 0) {
            return serializer.error(
                    "wrong number of arguments for XADD"
            );
        }

        String streamName = args.get(0);
        String requestedId = args.get(1);

        Map<String, String> fields = new LinkedHashMap<>();

        for (int i = 2; i < args.size(); i += 2) {
            fields.put(args.get(i), args.get(i + 1));
        }

        try {

            String actualId = store.add(
                    streamName,
                    requestedId,
                    fields
            );

            return serializer.bulkString(actualId);

        } catch (IllegalArgumentException e) {

            return serializer.error(e.getMessage());
        }
    }
}