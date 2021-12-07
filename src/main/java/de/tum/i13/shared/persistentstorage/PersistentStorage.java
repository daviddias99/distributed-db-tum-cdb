package de.tum.i13.shared.persistentstorage;

import de.tum.i13.shared.kv.KVMessage;
import de.tum.i13.shared.kv.KVStore;

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
