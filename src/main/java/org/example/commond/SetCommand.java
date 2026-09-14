package org.example.commond;


import org.example.Protocol.RespSerializer;
import org.example.storage.RedisStore;

import java.util.List;

public class SetCommand implements Command {

    private final RedisStore redisStore;

    private final RespSerializer serializer;


    public SetCommand(
            RedisStore redisStore,
            RespSerializer serializer
    ) {

        this.redisStore =
                redisStore;

        this.serializer =
                serializer;
    }


    @Override
    public byte[] execute(
            List<String> args
    ) {

        /*
         * SET key value
         *
         * args:
         * [key, value]
         */
        if (args.size() == 2) {

            String key =
                    args.get(0);

            String value =
                    args.get(1);

            redisStore.set(
                    key,
                    value
            );

            return serializer.simpleString(
                    "OK"
            );
        }


        /*
         * SET key value PX milliseconds
         *
         * args:
         * [key, value, PX, milliseconds]
         */
        if (args.size() == 4) {

            String key =
                    args.get(0);

            String value =
                    args.get(1);

            String option =
                    args.get(2);

            String expiryValue =
                    args.get(3);


            if (!option.equalsIgnoreCase("PX")) {

                return serializer.error(
                        "unsupported SET option"
                );
            }


            long ttlMillis;

            try {

                ttlMillis =
                        Long.parseLong(
                                expiryValue
                        );

            } catch (NumberFormatException e) {

                return serializer.error(
                        "invalid expire time"
                );
            }


            if (ttlMillis <= 0) {

                return serializer.error(
                        "invalid expire time"
                );
            }


            redisStore.set(
                    key,
                    value,
                    ttlMillis
            );


            return serializer.simpleString(
                    "OK"
            );
        }


        return serializer.error(
                "wrong number of arguments for SET"
        );
    }
}