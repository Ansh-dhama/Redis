package org.example.replication;

import org.example.Protocol.RespSerializer;
import org.example.commond.Command;
import org.example.config.RedisConfig;
import org.example.replication.ReplicationManager;

import java.util.List;

public class WaitCommand
        implements Command {

    private final RespSerializer serializer;

    private final RedisConfig config;

    private final ReplicationManager replicationManager;


    public WaitCommand(
            RespSerializer serializer,
            RedisConfig config,
            ReplicationManager replicationManager
    ) {

        this.serializer =
                serializer;

        this.config =
                config;

        this.replicationManager =
                replicationManager;
    }


    @Override
    public byte[] execute(
            List<String> args
    ) {

        if (args.size() != 2) {

            return serializer.error(
                    "wrong number of arguments for WAIT"
            );
        }


        if (!config.isMaster()) {

            return serializer.integer(
                    0
            );
        }


        try {

            int requiredReplicas =
                    Integer.parseInt(
                            args.get(0)
                    );


            long timeout =
                    Long.parseLong(
                            args.get(1)
                    );


            /*
             * We want replicas to have processed
             * everything up to this position.
             */
            long targetOffset =
                    replicationManager
                            .getMasterOffset();


            int alreadyAcked =
                    replicationManager
                            .countAckedReplicas(
                                    targetOffset
                            );


            if (
                    alreadyAcked
                            >= requiredReplicas
            ) {

                return serializer.integer(
                        alreadyAcked
                );
            }


            /*
             * Ask replicas:
             *
             * REPLCONF GETACK *
             */
            replicationManager
                    .requestAcks();


            int result =
                    replicationManager
                            .waitForReplicas(
                                    requiredReplicas,
                                    targetOffset,
                                    timeout
                            );


            return serializer.integer(
                    result
            );


        } catch (
                NumberFormatException e
        ) {

            return serializer.error(
                    "invalid WAIT arguments"
            );
        }
    }
}