    package org.example.Server;

    import org.example.Protocol.RespParser;
    import org.example.commond.CommandDispatcher;
    import org.example.config.RedisConfig;
    import org.example.persistence.RdbWriter;
    import org.example.replication.ReplicationManager;
    import org.example.storage.RedisStore;

    import java.io.*;
    import java.net.Socket;
    import java.nio.charset.StandardCharsets;
    import java.nio.file.Files;
    import java.util.List;
    import org.example.commond.RespCommandEncoder;

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

                    // Replica requests initial synchronization
                    if (config.isMaster() && command.equals("PSYNC")) {
                        handlePsync(output);
                        continue;
                    }

                    // Master receives ACK from replica
                    if (config.isMaster() && isAck(request)) {

                        long offset = Long.parseLong(request.get(2));

                        replicationManager.updateAck(
                                socket,  // FIX: use socket, not clientSocket
                                offset
                        );

                        continue;
                    }

                    // Execute normal commands
                    byte[] response = dispatcher.dispatch(request);

                    // Send successful SET commands to replicas
                    if (config.isMaster()
                            && command.equals("SET")
                            && response.length > 0
                            && response[0] != '-') {

                        replicationManager.propagate(
                                RespCommandEncoder.encode(request)
                        );
                    }

                    // Send response ONCE
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


        private boolean isAck(
                List<String> request
        ) {

            return request.size() >= 3
                    && request.get(0)
                    .equalsIgnoreCase("REPLCONF")
                    && request.get(1)
                    .equalsIgnoreCase("ACK");
        }


        private void handlePsync(OutputStream output) throws IOException {

            // Convert master's RAM data into an RDB file
            rdbWriter.save(config.rdbPath(), store);

            byte[] rdb = Files.readAllBytes(config.rdbPath());

            String fullResync =
                    "+FULLRESYNC "
                            + replicationManager.getReplicationId()
                            + " "
                            + replicationManager.getMasterOffset()
                            + "\r\n";

            synchronized (output) {

                output.write(
                        fullResync.getBytes(StandardCharsets.UTF_8)
                );

                // Tell replica how many RDB bytes are coming
                output.write(
                        ("$" + rdb.length + "\r\n")
                                .getBytes(StandardCharsets.UTF_8)
                );

                // Send actual RDB bytes
                output.write(rdb);
                output.flush();

                // Register replica for future SET commands
                replicationManager.registerReplica(socket, output);
            }

            System.out.println("FULLRESYNC sent");
        }
    };