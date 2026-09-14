package org.example.storage;

public class RedisEntry {

    private final String value;

    private final Long expireAt;

    public RedisEntry(
            String value,
            Long expireAt
    ) {
        this.value = value;
        this.expireAt = expireAt;
    }

    public String getValue() {
        return value;
    }

    public Long getExpireAt() {
        return expireAt;
    }

    public boolean hasExpiry() {
        return expireAt != null;
    }

    public boolean isExpired() {

        return expireAt != null
                && System.currentTimeMillis()
                >= expireAt;
    }
}