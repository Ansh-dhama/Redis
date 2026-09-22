package org.example.Stream;

import org.example.Protocol.RespSerializer;
import org.example.commond.Command;

import java.util.List;

public class XRangeCommand implements Command {

    private final StreamStore store;
    private final RespSerializer serializer;

    public XRangeCommand(
            StreamStore store,
            RespSerializer serializer
    ) {
        this.store = store;
        this.serializer = serializer;
    }

    @Override
    public byte[] execute(List<String> args) {

        if (args.size() != 3) {
            return serializer.error(
                    "wrong number of arguments for XRANGE"
            );
        }

        try {
            return StreamReply.range(
                    store.range(
                            args.get(0),  // stream name
                            args.get(1),  // start ID
                            args.get(2)   // end ID
                    )
            );

        } catch (NumberFormatException e) {
            return serializer.error("invalid stream ID");
        }
    }
}