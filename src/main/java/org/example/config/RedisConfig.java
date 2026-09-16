package org.example.config;

import java.nio.file.Path;

public record RedisConfig(

        int port,

        String dir,

        String dbFilename,

        String masterHost,

        Integer masterPort

) {

    public static RedisConfig fromArgs(
            String[] args
    ) {

        int port = 6379;

        String dir = ".";

        String dbFilename =
                "dump.rdb";

        String masterHost =
                null;

        Integer masterPort =
                null;


        for (
                int i = 0;
                i < args.length;
                i++
        ) {

            switch (args[i]) {

                case "--port" -> {

                    port =
                            Integer.parseInt(
                                    args[++i]
                            );
                }


                case "--dir" -> {

                    dir =
                            args[++i];
                }


                case "--dbfilename" -> {

                    dbFilename =
                            args[++i];
                }


                case "--replicaof" -> {

                    masterHost =
                            args[++i];

                    masterPort =
                            Integer.parseInt(
                                    args[++i]
                            );
                }
            }
        }


        return new RedisConfig(

                port,

                dir,

                dbFilename,

                masterHost,

                masterPort
        );
    }


    public Path rdbPath() {

        return Path.of(
                dir,
                dbFilename
        );
    }


    public boolean isReplica() {

        return masterHost != null
                && masterPort != null;
    }


    public boolean isMaster() {

        return !isReplica();
    }
}