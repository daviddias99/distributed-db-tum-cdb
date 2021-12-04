package de.tum.i13.server.cache;

import de.tum.i13.shared.kv.KVMessage;
import de.tum.i13.shared.kv.KVStore;

/**
 * An interface for a cache with altered exceptions compared to {@link KVStore}
 */
public interface Cache extends KVStore {

    /**
     * @param key {@inheritDoc} must not be null
     */
    @Override
    KVMessage get(String key);

    /**
     * @param key {@inheritDoc} must not be null
     */
    @Override
    KVMessage put(String key, String value);

    /**
     * Get the {@link CachingStrategy} of this {@link Cache}
     *
     * @return the {@link CachingStrategy} this {@link Cache} uses
     */
    CachingStrategy getCachingStrategy();

}
