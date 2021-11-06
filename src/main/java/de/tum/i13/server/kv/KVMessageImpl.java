package de.tum.i13.server.kv;

import de.tum.i13.shared.Preconditions;

public class KVMessageImpl implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;

    public KVMessageImpl(String key, StatusType status) {
        Preconditions.notNull(key);
        Preconditions.check(status.canHaveEmptyValue());
        Preconditions.notNull(status);

        this.key = key;
        this.value = null;
        this.status = status;
    }

    public KVMessageImpl(String key, String value, StatusType status) {
        Preconditions.notNull(key);
        Preconditions.notNull(value);
        Preconditions.notNull(status);

        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override

    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }

}
