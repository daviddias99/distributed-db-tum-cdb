package de.tum.i13.server.kv;

import de.tum.i13.shared.Preconditions;

/**
 * Basic data class implementation of {@link KVMessage}
 */
public class KVMessageImpl implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;

    /**
     * Creates a new {@link KVMessage} with empty value
     *
     * @param key the key to set, must not be null
     * @param status the status to set, must not be null
     */
    public KVMessageImpl(String key, StatusType status) {
        Preconditions.notNull(key, "Key cannot be null");
        Preconditions.notNull(status, "Status cannot be null");
        Preconditions.check(status.canHaveEmptyValue(), "Status cannot have an empty value");

        this.key = key;
        this.value = null;
        this.status = status;
    }

    // TODO Consider empty key in message packing and unpacking
    /**
     * Creates a new {@link KVMessage} with empty key and value
     *
     * @param status the status to set, must not be null
     */
    public KVMessageImpl(StatusType status) {
        Preconditions.notNull(status, "Status cannot be null");
        Preconditions.check(
                status == StatusType.UNDEFINED,
                "Status must be undefined to use this constructor");

        this.key = null;
        this.value = null;
        this.status = status;
    }

    /**
     * Creates a new {@link KVMessage} with present value
     *
     * @param key the key to set, must not be null
     * @param value the value to set, must not be null
     * @param status the status to set, must not be null
     */
    public KVMessageImpl(String key, String value, StatusType status) {
        Preconditions.notNull(key, "Key cannot be null");
        Preconditions.notNull(value, "Value cannot be null");
        Preconditions.notNull(status, "Status cannot be null");

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

    @Override
    public String toString() {
        return packMessage();
    }

}