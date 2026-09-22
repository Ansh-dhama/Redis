package org.example.Server;

import org.example.Protocol.RespParser;
import org.example.Stream.TransactionProcessor;
import org.example.commond.CommandDispatcher;
import org.example.commond.RespCommandEncoder;
import org.example.config.RedisConfig;
import org.example.persistence.RdbWriter;
import org.example.replication.ReplicationManager;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandDispatcher dispatcher;
    private final RedisConfig config;
    private final RedisStore store;
    private final RdbWriter rdbWriter;
    private final ReplicationManager replicationManager;

    public ClientHandler(
            Socket socket,
            CommandDispatcher dispatcher,
            RedisConfig config,
            RedisStore store,
            RdbWriter rdbWriter,
            ReplicationManager replicationManager
    ) {
        this.socket = socket;
        this.dispatcher = dispatcher;
        this.config = config;
        this.store = store;
        this.rdbWriter = rdbWriter;
        this.replicationManager = replicationManager;
    }

    @Override
    public void run() {

        RespParser parser = new RespParser();

        // Every client gets its OWN transaction queue.
        TransactionProcessor transaction =
                new TransactionProcessor(dispatcher);

        try (
                socket;
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream()
        ) {

            while (true) {

                List<String> request = parser.parse(input);

                if (request == null || request.isEmpty()) {
                    break;
                }

                String command = request.get(0).toUpperCase();

                // Phase 5: Initial replica synchronization
                if (config.isMaster() && command.equals("PSYNC")) {

                    handlePsync(output);
                    continue;
                }

                // Phase 5: Master receives ACK from replica
                if (config.isMaster() && isAck(request)) {

                    long offset = Long.parseLong(request.get(2));

                    replicationManager.updateAck(socket, offset);
                    continue;
                }

                // Phase 6: Handle normal commands OR MULTI/EXEC
                byte[] response = transaction.process(
                        request,
                        this::executeCommand
                );

                output.write(response);
                output.flush();
            }

        } catch (Exception e) {

            System.out.println(
                    "Client disconnected: " + e.getMessage()
            );

        } finally {

            replicationManager.removeReplica(socket);
        }
    }

    private byte[] executeCommand(List<String> request) {

        String command = request.get(0).toUpperCase();

        if (config.isReplica() &&
                (command.equals("SET") || command.equals("XADD"))) {

            return "-READONLY You can't write against a read only replica.\r\n"
                    .getBytes(StandardCharsets.UTF_8);
        }

        byte[] response = dispatcher.dispatch(request);

        if (config.isMaster() &&
                response.length > 0 &&
                response[0] != '-') {

            // Existing SET replication
            if (command.equals("SET")) {

                replicationManager.propagate(
                        RespCommandEncoder.encode(request)
                );
            }

            // New XADD replication
            if (command.equals("XADD")) {

                // XADD response contains the generated stream ID.
                String reply = new String(
                        response,
                        StandardCharsets.UTF_8
                );

                int headerEnd = reply.indexOf("\r\n");

                String actualId = reply.substring(
                        headerEnd + 2,
                        reply.length() - 2
                );

                List<String> replicated =
                        new java.util.ArrayList<>(request);

                // Replace "*" with master's generated ID.
                replicated.set(2, actualId);

                replicationManager.propagate(
                        RespCommandEncoder.encode(replicated)
                );
            }
        }

        return response;
    }
    private boolean isAck(List<String> request) {

        return request.size() == 3
                && request.get(0).equalsIgnoreCase("REPLCONF")
                && request.get(1).equalsIgnoreCase("ACK");
    }
    private void handlePsync(OutputStream output) throws IOException {

        synchronized (dispatcher) {

            // Existing string data
            rdbWriter.save(config.rdbPath(), store);

            byte[] rdb = Files.readAllBytes(
                    config.rdbPath()
            );

            // Existing stream data
            List<List<String>> streamCommands =
                    dispatcher.getStreamStore().snapshotCommands();

            String fullResync =
                    "+FULLRESYNC "
                            + replicationManager.getReplicationId()
                            + " "
                            + replicationManager.getMasterOffset()
                            + "\r\n";

            synchronized (output) {

                // 1. FULLRESYNC response
                output.write(
                        fullResync.getBytes(StandardCharsets.UTF_8)
                );

                // 2. String RDB
                output.write(
                        ("$" + rdb.length + "\r\n")
                                .getBytes(StandardCharsets.UTF_8)
                );

                output.write(rdb);

                // 3. Tell replica how many stream entries follow
                output.write(
                        ("+STREAMSYNC " + streamCommands.size() + "\r\n")
                                .getBytes(StandardCharsets.UTF_8)
                );

                // 4. Send existing stream entries
                for (List<String> command : streamCommands) {

                    output.write(
                            RespCommandEncoder.encode(command)
                    );
                }

                output.flush();

                // 5. Register replica for future commands
                replicationManager.registerReplica(socket, output);
            }
        }

        System.out.println("FULLRESYNC and STREAMSYNC sent");
    }
}