package org.example.storage;

import org.example.storage.RedisEntry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RedisStore {

    private final ConcurrentMap<String, RedisEntry> data =
            new ConcurrentHashMap<>();


    public void set(
            String key,
            String value
    ) {

        RedisEntry entry =
                new RedisEntry(
                        value,
                        null
                );

        data.put(
                key,
                entry
        );
    }


    public void set(
            String key,
            String value,
            long ttlMillis
    ) {

        long expireAt =
                System.currentTimeMillis()
                        + ttlMillis;

        RedisEntry entry =
                new RedisEntry(
                        value,
                        expireAt
                );

        data.put(
                key,
                entry
        );
    }


    public String get(String key) {

        RedisEntry entry =
                data.get(key);

        if (entry == null) {
            return null;
        }

        if (entry.isExpired()) {

            data.remove(
                    key,
                    entry
            );

            return null;
        }

        return entry.getValue();
    }
}