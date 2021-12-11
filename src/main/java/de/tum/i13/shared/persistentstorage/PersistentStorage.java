package de.tum.i13.shared.persistentstorage;

import java.util.List;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;

/**
 * An interface for a persistent storage with altered exceptions compared to
 * {@link KVStore}
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

    /**
     * Get elements of storage that contain keys in range [lowerBound-upperBound]
     * (limits included).
     * 
     * @param lowerBound lower bound of keys
     * @param upperBound upper bound of keys
     * @return elements with keys in interval [lowerBound-upperBound]
     * @throws GetException an exception is thrown if any error occurs with fetching
     *                      the elements
     */
    List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException;
}
