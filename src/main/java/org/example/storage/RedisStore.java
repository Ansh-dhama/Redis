package org.example.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RedisStore {

    private final ConcurrentMap<String, RedisEntry> data =
            new ConcurrentHashMap<>();


    public void set(
            String key,
            String value
    ) {

        data.put(
                key,
                new RedisEntry(
                        value,
                        null
                )
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

        data.put(
                key,
                new RedisEntry(
                        value,
                        expireAt
                )
        );
    }


    // Used when loading RDB.
    // RDB already stores absolute expiry.
    public void restore(
            String key,
            String value,
            Long expireAt
    ) {

        if (expireAt != null
                && System.currentTimeMillis()
                >= expireAt) {

            return;
        }

        data.put(
                key,
                new RedisEntry(
                        value,
                        expireAt
                )
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

    public Map<String, RedisEntry> snapshot() {

        Map<String, RedisEntry> snapshot =
                new HashMap<>();

        for (var item : data.entrySet()) {

            String key =
                    item.getKey();

            RedisEntry entry =
                    item.getValue();

            if (entry.isExpired()) {

                data.remove(
                        key,
                        entry
                );

                continue;
            }

            snapshot.put(
                    key,
                    entry
            );
        }

        return snapshot;
    }
    public List<String> keys() {

        List<String> keys =
                new ArrayList<>();

        for (var item : data.entrySet()) {

            String key =
                    item.getKey();

            RedisEntry entry =
                    item.getValue();

            if (entry.isExpired()) {

                data.remove(
                        key,
                        entry
                );

                continue;
            }

            keys.add(key);
        }

        return keys;
    }
}