package org.example.Server;


import org.example.Protocol.RespSerializer;
import org.example.commond.CommandDispatcher;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {

    private final int port;

    private final RedisStore redisStore;

    private final RespSerializer serializer;

    private final CommandDispatcher dispatcher;

    public RedisServer(int port) {

        this.port = port;

        this.redisStore =
                new RedisStore();

        this.serializer =
                new RespSerializer();

        this.dispatcher =
                new CommandDispatcher(
                        redisStore,
                        serializer
                );
    }

    public void start() {

        ExecutorService executor =
                Executors.newCachedThreadPool();

        try (
                ServerSocket serverSocket =
                        new ServerSocket(port)
        ) {

            System.out.println(
                    "Mini Redis started on port "
                            + port
            );

            while (true) {

                Socket clientSocket =
                        serverSocket.accept();

                System.out.println(
                        "Client connected: "
                                + clientSocket
                                .getRemoteSocketAddress()
                );

                executor.submit(
                        new ClientHandler(
                                clientSocket,
                                dispatcher
                        )
                );
            }

        } catch (IOException e) {

            throw new RuntimeException(
                    "Unable to start Redis server",
                    e
            );
        }
    }
}