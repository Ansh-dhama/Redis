package org.example.replication;

import org.example.config.RedisConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ReplicaClient {

    private final RedisConfig config;

    private Socket socket;
    private InputStream input;
    private OutputStream output;


    public ReplicaClient(RedisConfig config) {
        this.config = config;
    }


    public void connect() {

        try {

            // Connect replica -> master
            socket = new Socket();

            socket.connect(
                    new InetSocketAddress(
                            config.masterHost(),
                            config.masterPort()
                    )
            );

            input = socket.getInputStream();
            output = socket.getOutputStream();

            System.out.println(
                    "Connected to master "
                            + config.masterHost()
                            + ":"
                            + config.masterPort()
            );


            // STEP 1: PING
            sendCommand("PING");

            System.out.println(
                    "PING -> " + readLine()
            );


            // STEP 2: Tell master replica port
            sendCommand(
                    "REPLCONF",
                    "listening-port",
                    String.valueOf(config.port())
            );

            System.out.println(
                    "REPLCONF port -> "
                            + readLine()
            );


            // STEP 3: Tell master capability
            sendCommand(
                    "REPLCONF",
                    "capa",
                    "psync2"
            );

            System.out.println(
                    "REPLCONF capa -> "
                            + readLine()
            );


        } catch (IOException e) {

            System.out.println(
                    "Unable to connect to master: "
                            + e.getMessage()
            );
        }
    }


    /*
     * Convert Java command into RESP
     *
     * sendCommand("PING")
     *
     * becomes:
     *
     * *1
     * $4
     * PING
     */
    private void sendCommand(
            String... parts
    ) throws IOException {

        StringBuilder command =
                new StringBuilder();

        command.append("*")
                .append(parts.length)
                .append("\r\n");


        for (String part : parts) {

            byte[] bytes =
                    part.getBytes(
                            StandardCharsets.UTF_8
                    );

            command.append("$")
                    .append(bytes.length)
                    .append("\r\n")
                    .append(part)
                    .append("\r\n");
        }


        output.write(
                command.toString()
                        .getBytes(
                                StandardCharsets.UTF_8
                        )
        );

        output.flush();
    }


    /*
     * Read RESP simple response:
     *
     * +PONG\r\n
     * +OK\r\n
     */
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

                line.append((char) current);
                line.append((char) next);

            } else {

                line.append(
                        (char) current
                );
            }
        }


        return line.toString();
    }
}