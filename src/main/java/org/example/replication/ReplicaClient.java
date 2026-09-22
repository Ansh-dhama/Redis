package org.example.replication;

import org.example.Protocol.RespParser;
import org.example.commond.CommandDispatcher;
import org.example.config.RedisConfig;
import org.example.persistence.RdbParser;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.net.Socket;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.example.commond.RespCommandEncoder;
import java.util.List;

public class ReplicaClient {

    private final RedisConfig config;

    private final CommandDispatcher dispatcher;

    private final RedisStore redisStore;


    private Socket socket;

    private InputStream input;

    private OutputStream output;


    private long replicationOffset =
            0;


    private final RespParser parser =
            new RespParser();


    public ReplicaClient(
            RedisConfig config,
            CommandDispatcher dispatcher,
            RedisStore redisStore
    ) {

        this.config =
                config;

        this.dispatcher =
                dispatcher;

        this.redisStore =
                redisStore;
    }


    public void connect() {

        try {

            socket =
                    new Socket();


            socket.connect(
                    new InetSocketAddress(
                            config.masterHost(),
                            config.masterPort()
                    )
            );


            input =
                    socket.getInputStream();

            output =
                    socket.getOutputStream();


            System.out.println(
                    "Connected to master "
                            + config.masterHost()
                            + ":"
                            + config.masterPort()
            );


            /*
             * STEP 1
             */
            sendCommand(
                    "PING"
            );

            System.out.println(
                    readLine()
            );


            /*
             * STEP 2
             */
            sendCommand(
                    "REPLCONF",
                    "listening-port",
                    String.valueOf(
                            config.port()
                    )
            );

            System.out.println(
                    readLine()
            );


            /*
             * STEP 3
             */
            sendCommand(
                    "REPLCONF",
                    "capa",
                    "psync2"
            );

            System.out.println(
                    readLine()
            );


            /*
             * STEP 4
             *
             * We are new.
             * Request FULL sync.
             */
            sendCommand(
                    "PSYNC",
                    "?",
                    "-1"
            );


            /*
             * +FULLRESYNC id offset
             */
            String fullResync =
                    readLine();


            System.out.println(
                    fullResync
            );


            parseFullResync(
                    fullResync
            );


            /*
             * Receive RDB.
             */
            receiveRdb();

            receiveStreams();

            replicationLoop();

        } catch (Exception e) {

            System.out.println(
                    "Replication error: "
                            + e.getMessage()
            );
        }
    }


    private void parseFullResync(
            String response
    ) throws IOException {

        if (!response.startsWith(
                "+FULLRESYNC"
        )) {

            throw new IOException(
                    "Expected FULLRESYNC but received "
                            + response
            );
        }


        String[] parts =
                response.split(" ");


        replicationOffset =
                Long.parseLong(
                        parts[2]
                );
    }


    private void receiveRdb()
            throws IOException {

        /*
         * Example:
         *
         * $119
         */
        String rdbHeader =
                readLine();


        if (!rdbHeader.startsWith("$")) {

            throw new IOException(
                    "Expected RDB bulk length"
            );
        }


        int rdbLength =
                Integer.parseInt(
                        rdbHeader.substring(1)
                );


        byte[] rdbBytes =
                input.readNBytes(
                        rdbLength
                );


        if (
                rdbBytes.length
                        != rdbLength
        ) {

            throw new IOException(
                    "Incomplete RDB received"
            );
        }


        Path path =
                config.rdbPath();


        Path parent =
                path.getParent();


        if (parent != null) {

            Files.createDirectories(
                    parent
            );
        }


        Files.write(
                path,
                rdbBytes
        );


        /*
         * FULL sync replaces local data.
         */
        redisStore.clear();


        new RdbParser()
                .load(
                        path,
                        redisStore
                );


        System.out.println(
                "Replica loaded master RDB"
        );
    }


    private void replicationLoop() throws IOException {

        while (true) {

            List<String> command = parser.parse(input);

            if (command == null || command.isEmpty()) {
                break;
            }

            int commandBytes =
                    RespCommandEncoder.encode(command).length;

            // Master asks for replica's processed offset
            if (isGetAck(command)) {

                sendCommand(
                        "REPLCONF",
                        "ACK",
                        String.valueOf(replicationOffset)
                );

                replicationOffset += commandBytes;
                continue;
            }

            // Apply replicated SET to replica's RedisStore
            dispatcher.dispatch(command);

            replicationOffset += commandBytes;

            System.out.println("Replica applied: " + command);
        }
    }


    private boolean isGetAck(
            List<String> command
    ) {

        return command.size() >= 3

                && command.get(0)
                .equalsIgnoreCase(
                        "REPLCONF"
                )

                && command.get(1)
                .equalsIgnoreCase(
                        "GETACK"
                );
    }


    private void sendCommand(
            String... parts
    ) throws IOException {

        byte[] command =
                RespCommandEncoder.encode(
                        parts
                );


        output.write(
                command
        );

        output.flush();
    }


    private String readLine()
            throws IOException {

        StringBuilder line =
                new StringBuilder();


        while (true) {

            int current =
                    input.read();


            if (current == -1) {

                throw new IOException(
                        "Master closed connection"
                );
            }


            if (current == '\r') {

                int next =
                        input.read();


                if (next == '\n') {

                    break;
                }


                line.append(
                        (char) current
                );

                line.append(
                        (char) next
                );

            } else {

                line.append(
                        (char) current
                );
            }
        }


        return line.toString();
    }private void receiveStreams() throws IOException {

        String header = readLine();

        if (!header.startsWith("+STREAMSYNC ")) {

            throw new IOException(
                    "Expected STREAMSYNC, received: " + header
            );
        }

        int count = Integer.parseInt(
                header.substring("+STREAMSYNC ".length())
        );

        // Full synchronization replaces old stream data.
        dispatcher.getStreamStore().clear();

        for (int i = 0; i < count; i++) {

            List<String> command = parser.parse(input);

            if (command == null ||
                    command.isEmpty() ||
                    !command.get(0).equalsIgnoreCase("XADD")) {

                throw new IOException(
                        "Invalid stream synchronization entry"
                );
            }

            byte[] result = dispatcher.dispatch(command);

            if (result.length == 0 || result[0] == '-') {

                throw new IOException(
                        "Failed to restore stream entry: "
                                + command
                                + " | Error: "
                                + new String(result, StandardCharsets.UTF_8)
                );
            }
        }

        System.out.println(
                "Replica synchronized " + count + " stream entries"
        );
    }
}