package de.tum.i13.server.cache;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * A persistent storage with a cache layer in front of the storage
 */
public class CachedPersistentStorage implements PersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(CachedPersistentStorage.class);
    private final Cache cache;
    private final PersistentStorage persistentStorage;

    /**
     * Constructs a new storage with the given storage and caching configuration
     *
     * @param persistentStorage the {@link PersistentStorage} to use as a backend, must not be null
     * @param cachingStrategy the {@link CachingStrategy} to use, most not be null
     * @param cacheSize the size of the cache, must be greater than 0
     */
    public CachedPersistentStorage(PersistentStorage persistentStorage, CachingStrategy cachingStrategy,
                                   int cacheSize) {
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
        LOGGER.info("Trying to put key {} with value {}", key, value);

        try {
            if (value == null) LOGGER.debug("Deleting key {}", key);
            else LOGGER.debug("Putting key {} to value {}", key, value);
            persistentStorage.put(key, value);
            cache.put(key, value);
            return Optional.ofNullable(value)
                    .map(newValue -> new KVMessageImpl(key, newValue, KVMessage.StatusType.PUT_SUCCESS))
                    .orElseGet(() -> new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS));
        } catch (Exception exception) {
            final PutException putException = new PutException(
                    exception,
                    "Could not put key %s with value %s into persistent storage",
                    key,
                    value
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw putException;
        }
    }

    @Override
    public KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        final KVMessage cacheResponse = cache.get(key);
        final KVMessage.StatusType cacheGetStatus = cacheResponse.getStatus();
        if (cacheGetStatus == KVMessage.StatusType.GET_SUCCESS) {
            LOGGER.debug("Found key {} with value {} in cache", key, cacheResponse.getValue());
            return cacheResponse;
        } else if (cacheGetStatus == KVMessage.StatusType.GET_ERROR) {
            return handleCacheMiss(key);
        } else {
            final GetException getException = new GetException(
                    "Cache returned unprocessable status code %s while getting key %s",
                    cacheGetStatus,
                    key
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
            throw getException;
        }
    }

    private KVMessage handleCacheMiss(String key) throws GetException {
        LOGGER.debug("Handling cache miss of key {}", key);
        try {
            final KVMessage storageResponse = persistentStorage.get(key);
            final KVMessage.StatusType storageStatus = storageResponse.getStatus();
            if (storageStatus == KVMessage.StatusType.GET_SUCCESS) {
                final String storageValue = storageResponse.getValue();
                LOGGER.debug("Found key {} with value {} in persistent storage", key, storageValue);
                return updateCache(key, storageValue);
            } else if (storageStatus == KVMessage.StatusType.GET_ERROR) {
                LOGGER.debug("Did not found key {} in persistent storage", key);
                return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
            } else {
                final GetException getException = new GetException(
                        "Persistent storage returned unprocessable status code %s while getting key %s",
                        storageStatus,
                        key
                );
                LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
                throw getException;
            }
        } catch (GetException exception) {
            final GetException getException = new GetException(
                    exception,
                    "Could not get key %s from persistent storage",
                    key
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
            throw getException;
        }
    }

    private KVMessage updateCache(String key, String storageValue) throws GetException {
        LOGGER.debug("Updating cache with key {} and value {}", key, storageValue);
        final KVMessage cachePutResponse = cache.put(key, storageValue);
        final KVMessage.StatusType cachePutStatus = cachePutResponse.getStatus();
        if (cachePutStatus == KVMessage.StatusType.PUT_SUCCESS) {
            LOGGER.debug("Successfully updated cache with key {} and value {}", key, storageValue);
            return new KVMessageImpl(key, storageValue, KVMessage.StatusType.GET_SUCCESS);
        } else {
            final GetException getException = new GetException(
                    "Cache returned unprocessable status code %s while putting key %s with value %s",
                    cachePutStatus,
                    key,
                    storageValue
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
            throw getException;
        }
    }

}
