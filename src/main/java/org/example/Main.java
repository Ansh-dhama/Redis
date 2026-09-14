package org.example;


import org.example.Server.RedisServer;

public class Main {

    public static void main(String[] args) {

        RedisServer redisServer =
                new RedisServer(6379);

        redisServer.start();
    }
}