package org.example.Server;

import org.example.Protocol.RespSerializer;
import org.example.commond.CommandDispatcher;
import org.example.config.RedisConfig;
import org.example.persistence.RdbParser;
import org.example.persistence.RdbWriter;
import org.example.replication.ReplicaClient;
import org.example.replication.ReplicationManager;
import org.example.storage.RedisStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {

    private final RedisConfig config;

    private final RedisStore redisStore;

    private final RespSerializer serializer;

    private final ReplicationManager replicationManager;

    private final CommandDispatcher dispatcher;

    private ReplicaClient replicaClient;


    public RedisServer(
            RedisConfig config
    ) {

        this.config = config;

        this.redisStore =
                new RedisStore();

        this.serializer =
                new RespSerializer();

        this.replicationManager =
                new ReplicationManager();


        this.dispatcher =
                new CommandDispatcher(
                        redisStore,
                        serializer,
                        config,
                        replicationManager
                );


        if (config.isReplica()) {

            this.replicaClient =
                    new ReplicaClient(
                            config,
                            dispatcher,
                            redisStore
                    );
        }
    }


    public void start() {

        // Load RDB first
        loadRdb();


        // Replica connects to master
        if (config.isReplica()) {

            Thread replicaThread =
                    new Thread(
                            replicaClient::connect
                    );

            replicaThread.setName(
                    "redis-replication"
            );

            replicaThread.start();
        }


        ExecutorService executorService =
                Executors.newCachedThreadPool();


        try (
                ServerSocket serverSocket =
                        new ServerSocket(
                                config.port()
                        )
        ) {

            System.out.println(
                    "Mini Redis started on port "
                            + config.port()
            );


            while (true) {

                Socket client =
                        serverSocket.accept();


                executorService.submit(
                        new ClientHandler(
                                client,
                                dispatcher,
                                config,
                                redisStore,
                                new RdbWriter(),
                                replicationManager
                        )
                );
            }

        } catch (IOException e) {

            throw new RuntimeException(e);
        }
    }


    private void loadRdb() {

        RdbParser parser =
                new RdbParser();


        try {

            System.out.println(
                    "Loading RDB from: "
                            + config.rdbPath()
            );


            parser.load(
                    config.rdbPath(),
                    redisStore
            );


            System.out.println(
                    "RDB loading completed."
            );


        } catch (IOException e) {

            throw new RuntimeException(
                    "Unable to load RDB file",
                    e
            );
        }
    }
}