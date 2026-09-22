package org.example.Stream;

import org.example.Protocol.RespSerializer;
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

        // XADD orders * item pizza
        if (args.size() < 4 || args.size() % 2 != 0) {
            return serializer.error("wrong number of arguments for XADD");
        }

        String streamName = args.get(0);
        String requestedId = args.get(1);

        // For now, support only automatic ID generation.
        if (!requestedId.equals("*")) {
            return serializer.error("only * IDs are supported");
        }

        Map<String, String> fields = new LinkedHashMap<>();

        for (int i = 2; i < args.size(); i += 2) {
            fields.put(args.get(i), args.get(i + 1));
        }

        String generatedId = store.add(streamName, fields);

        return serializer.bulkString(generatedId);
    }
}