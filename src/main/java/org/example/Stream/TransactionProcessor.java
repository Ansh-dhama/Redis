package org.example.Stream;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public class TransactionProcessor {

    private final TransactionState state =
            new TransactionState();

    // Shared dispatcher object acts as our command lock
    private final Object commandLock;


    public TransactionProcessor(Object commandLock) {
        this.commandLock = commandLock;
    }


    public byte[] process(
            List<String> request,
            Function<List<String>, byte[]> execute
    ) {

        if (request == null || request.isEmpty()) {
            return reply("-ERR empty command\r\n");
        }

        String name = request.get(0).toUpperCase();

        // MULTI starts transaction mode
        if (name.equals("MULTI")) {

            if (request.size() != 1) {
                return reply("-ERR wrong number of arguments\r\n");
            }

            if (state.isActive()) {
                return reply("-ERR MULTI calls can not be nested\r\n");
            }

            state.begin();
            return reply("+OK\r\n");
        }


        // DISCARD cancels a transaction
        if (name.equals("DISCARD")) {

            if (!state.isActive()) {
                return reply("-ERR DISCARD without MULTI\r\n");
            }

            state.discard();
            return reply("+OK\r\n");
        }


        // EXEC executes queued commands
        if (name.equals("EXEC")) {

            if (!state.isActive()) {
                return reply("-ERR EXEC without MULTI\r\n");
            }

            List<List<String>> commands = state.drain();

            synchronized (commandLock) {

                ByteArrayOutputStream out =
                        new ByteArrayOutputStream();

                out.writeBytes(
                        reply("*" + commands.size() + "\r\n")
                );

                for (List<String> command : commands) {
                    out.writeBytes(execute.apply(command));
                }

                return out.toByteArray();
            }
        }


        // While transaction is active, queue commands
        if (state.isActive()) {

            state.add(request);
            return reply("+QUEUED\r\n");
        }


        // XREAD BLOCK must not hold the shared command lock
        if (name.equals("XREAD")) {
            return execute.apply(request);
        }

        synchronized (commandLock) {
            return execute.apply(request);
        }
    }


    private byte[] reply(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}