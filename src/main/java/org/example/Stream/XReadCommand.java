package org.example.Stream;

import org.example.Protocol.RespSerializer;
import org.example.commond.Command;

import java.util.List;

public class XReadCommand implements Command {

    private final StreamStore store;
    private final RespSerializer serializer;

    public XReadCommand(
            StreamStore store,
            RespSerializer serializer
    ) {
        this.store = store;
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(List<String> args) {

        long blockMs = -1; // -1 means do not wait
        int index = 0;

        try {

            // Optional: BLOCK 5000
            if (!args.isEmpty()
                    && args.get(0).equalsIgnoreCase("BLOCK")) {

                if (args.size() < 2) {
                    return serializer.error("missing BLOCK timeout");
                }

                blockMs = Long.parseLong(args.get(1));

                if (blockMs < 0) {
                    return serializer.error("invalid BLOCK timeout");
                }

                index = 2;
            }

            // Required: STREAMS orders <lastId>
            if (args.size() != index + 3
                    || !args.get(index).equalsIgnoreCase("STREAMS")) {

                return serializer.error("invalid XREAD arguments");
            }

            String streamName = args.get(index + 1);
            String lastId = args.get(index + 2);

            List<StreamEntry> entries = store.readAfter(
                    streamName,
                    lastId,
                    blockMs
            );

            if (entries.isEmpty()) {
                return StreamReply.nullReply();
            }

            return StreamReply.read(streamName, entries);

        } catch (NumberFormatException e) {

            return serializer.error("invalid XREAD ID or timeout");

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            return serializer.error("XREAD interrupted");
        }
    }
}