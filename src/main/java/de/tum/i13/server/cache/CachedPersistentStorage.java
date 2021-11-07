package de.tum.i13.server.cache;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Preconditions;

public class CachedPersistentStorage implements KVStore {

    private final Cache cache;
    private final PersistentStorage persistentStorage;

    public CachedPersistentStorage(PersistentStorage persistentStorage, CachingStrategy cachingStrategy, int cacheSize) {
        Preconditions.notNull(persistentStorage, "Persistent storage cannot be null");
        Preconditions.notNull(cachingStrategy, "Caching strategy cannot be null");
        Preconditions.check(cacheSize > 0, "Cache size must be greater than 0");

        this.persistentStorage = persistentStorage;
        this.cache = switch (cachingStrategy) {
            case LFU -> new LFUCache(cacheSize);
            case LRU -> new LRUCache(cacheSize);
            case FIFO -> new FIFOCache(cacheSize);
        };
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        Preconditions.notNull(key, "Key cannot be null");
        Preconditions.notNull(value, "Value cannot be null");

        try {
            persistentStorage.put(key, value);
            cache.put(key, value);
            return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
        } catch (Exception exception) {
            throw new PutException(
                    exception,
                    "Could not put key %s with value %s into persistent storage",
                    key,
                    value
            );
        }
    }

    @Override
    public KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");

        final KVMessage cacheResponse = cache.get(key);
        final KVMessage.StatusType cacheGetStatus = cacheResponse.getStatus();
        if (cacheGetStatus == KVMessage.StatusType.GET_SUCCESS) {
            return cacheResponse;
        }
        else if (cacheGetStatus == KVMessage.StatusType.GET_ERROR) {
            return handleCacheMiss(key);
        }
        else {
            throw new GetException(
                    "Cache returned unprocessable status code %s while getting key %s",
                    cacheGetStatus,
                    key
            );
        }
    }

    private KVMessage handleCacheMiss(String key) throws GetException {
        try {
            final KVMessage storageResponse = persistentStorage.get(key);
            final KVMessage.StatusType storageStatus = storageResponse.getStatus();
            if (storageStatus == KVMessage.StatusType.GET_SUCCESS) {
                return updateCache(key, storageResponse.getValue());
            } else if (storageStatus == KVMessage.StatusType.GET_ERROR) {
                return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
            }
            else {
                throw new GetException(
                        "Persistent storage returned unprocessable status code %s while getting key %s",
                        storageStatus,
                        key
                );
            }
        } catch (GetException exception) {
            throw new GetException(
                    exception,
                    "Could not get key %s from persistent storage",
                    key
            );
        }
    }

    private KVMessage updateCache(String key, String storageValue) throws GetException {
        final KVMessage cachePutResponse = cache.put(key, storageValue);
        final KVMessage.StatusType cachePutStatus = cachePutResponse.getStatus();
        if (cachePutStatus == KVMessage.StatusType.PUT_SUCCESS) {
            return new KVMessageImpl(key, storageValue, KVMessage.StatusType.GET_SUCCESS);
        }
        else {
            throw new GetException(
                    "Cache returned unprocessable status code %s while putting key %s with value %s",
                    cachePutStatus,
                    key,
                    storageValue
            );
        }
    }

}
