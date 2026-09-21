package org.example.replication;

import org.example.Protocol.RespSerializer;
import org.example.commond.Command;
import org.example.config.RedisConfig;
import org.example.replication.ReplicationManager;

import java.util.List;

public class InfoCommand
        implements Command {

    private final RespSerializer serializer;

    private final RedisConfig config;

    private final ReplicationManager replicationManager;


    public InfoCommand(
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

        StringBuilder info =
                new StringBuilder();


        if (config.isMaster()) {

            info.append("role:master\r\n");

            info.append(
                    "master_replid:"
            ).append(
                    replicationManager
                            .getReplicationId()
            ).append("\r\n");

            info.append(
                    "master_repl_offset:"
            ).append(
                    replicationManager
                            .getMasterOffset()
            ).append("\r\n");

            info.append(
                    "connected_slaves:"
            ).append(
                    replicationManager
                            .getReplicaCount()
            ).append("\r\n");

        } else {

            info.append(
                    "role:slave\r\n"
            );

            info.append(
                    "master_host:"
            ).append(
                    config.masterHost()
            ).append("\r\n");

            info.append(
                    "master_port:"
            ).append(
                    config.masterPort()
            ).append("\r\n");
        }


        return serializer.bulkString(
                info.toString()
        );
    }
}