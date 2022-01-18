package de.tum.i13.server.kvchord;

/**
 * Storer of KV elements
 */
public interface KVStore {

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key. If null, deletes the {@code key}.
     * @return a message that confirms the insertion of the tuple, the deletion of the key or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    KVMessage put(String key, String value) throws Exception;

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    KVMessage get(String key) throws Exception;

}
