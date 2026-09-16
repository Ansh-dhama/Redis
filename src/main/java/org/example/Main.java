package org.example;

import org.example.Server.RedisServer;
import org.example.config.RedisConfig;

public class Main {

    public static void main(String[] args) {

        RedisConfig config =
                RedisConfig.fromArgs(args);

        System.out.println(
                "RDB path = "
                        + config.rdbPath()
        );

        RedisServer redisServer =
                new RedisServer(config);

        redisServer.start();
    }
}