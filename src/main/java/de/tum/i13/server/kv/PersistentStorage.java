package de.tum.i13.server.kv;

/**
 * An interface for a persistent storage with altered exceptions compared to {@link KVStore}
 */
public interface PersistentStorage extends KVStore {

    /**
     * @param key {@inheritDoc} must not be null
     * @throws GetException if the retrieval of the key fails
     */
    @Override
    KVMessage get(String key) throws GetException;

    /**
     * @param key {@inheritDoc} must not be null
     * @throws PutException if the putting of the key fails
     */
    @Override
    KVMessage put(String key, String value) throws PutException;

}
