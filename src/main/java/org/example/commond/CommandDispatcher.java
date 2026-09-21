package org.example.commond;

import org.example.Protocol.RespSerializer;
import org.example.config.RedisConfig;
import org.example.persistence.RdbWriter;
import org.example.replication.InfoCommand;
import org.example.replication.ReplicationManager;
import org.example.replication.WaitCommand;
import org.example.storage.RedisStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandDispatcher {

    private final Map<String, Command> commands =
            new HashMap<>();

    private final RespSerializer serializer;


    public CommandDispatcher(
            RedisStore redisStore,
            RespSerializer serializer,
            RedisConfig config,
            ReplicationManager replicationManager
    ) {
        {

            this.serializer = serializer;


            commands.put(
                    "PING",
                    new PingCommand(
                            serializer
                    )
            );


            commands.put(
                    "ECHO",
                    new EchoCommand(
                            serializer
                    )
            );


            commands.put(
                    "SET",
                    new SetCommand(
                            redisStore,
                            serializer
                    )
            );


            commands.put(
                    "GET",
                    new GetCommand(
                            redisStore,
                            serializer
                    )
            );
            commands.put(
                    "INFO",
                    new InfoCommand(
                            serializer,
                            config,
                            replicationManager
                    )
            );

            commands.put(
                    "WAIT",
                    new WaitCommand(
                            serializer,
                            config,
                            replicationManager
                    )
            );
            // NEW
            commands.put(
                    "SAVE",
                    new SaveCommand(
                            redisStore,
                            new RdbWriter(),
                            config.rdbPath(),
                            serializer
                    )
            );
            commands.put(
                    "REPLCONF",
                    new ReplConfCommand(
                            serializer
                    )
            );
        }

    }
    public byte[] dispatch(
            List<String> request
    ) {

        if (request == null
                || request.isEmpty()) {

            return serializer.error(
                    "empty command"
            );
        }


        String commandName =
                request.get(0)
                        .toUpperCase();


        Command command =
                commands.get(
                        commandName
                );


        if (command == null) {

            return serializer.error(
                    "unknown command '"
                            + commandName
                            + "'"
            );
        }


        List<String> args =
                request.subList(
                        1,
                        request.size()
                );


        return command.execute(
                args
        );
    }
}