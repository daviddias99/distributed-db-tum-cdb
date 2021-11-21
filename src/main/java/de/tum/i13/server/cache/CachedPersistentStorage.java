package de.tum.i13.server.cache;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

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
     * @param cachingStrategy   the {@link CachingStrategy} to use, most not be null
     * @param cacheSize         the size of the cache, must be greater than 0
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
    public synchronized KVMessage put(String key, String value) throws PutException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to put key {} with value {}", key, value);

        try {
            final StatusType storageStatus = persistentStorage.put(key, value).getStatus();

            if (value == null) return finalizeDeletingKey(key, storageStatus);
            else return finalizePuttingKeyToValue(key, value, storageStatus);

        } catch (PutException exception) {
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

    /**
     * Updates the key in the cache with an actual present value based on the response status of the storage
     */
    private KVMessage finalizePuttingKeyToValue(String key, String value, StatusType storageStatus) throws PutException {
        LOGGER.debug("Putting key {} to value {}", key, value);
        final Set<StatusType> permittedStatuses = Set.of(StatusType.PUT_UPDATE, StatusType.PUT_SUCCESS,
                StatusType.PUT_ERROR);
        if (!permittedStatuses.contains(storageStatus)) {
            final PutException putException = new PutException(
                    "Persistent storage returned unprocessable status code %s while putting key %s to value %s",
                    storageStatus,
                    key,
                    value
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
            throw putException;
        } else if (storageStatus == StatusType.PUT_ERROR) {
            return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
        } else {
            final StatusType cacheStatus = cache.put(key, value).getStatus();
            if (!permittedStatuses.contains(cacheStatus)) {
                final PutException putException = new PutException(
                        "Cache storage returned unprocessable status code %s while putting key %s to value %s",
                        storageStatus,
                        key,
                        value
                );
                LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
                throw putException;
            }
            // Note that if an error happens the cache and the storage might diverge from each other
            else if (cacheStatus == StatusType.PUT_ERROR) return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
            else return new KVMessageImpl(key, value, storageStatus);
        }
    }

    /**
     * Deletes a key in the cache because of an absent value based on the response status of the storage
     */
    private KVMessageImpl finalizeDeletingKey(String key, StatusType storageStatus) throws PutException {
        LOGGER.debug("Deleting key {}", key);
        if (storageStatus != StatusType.DELETE_SUCCESS && storageStatus != StatusType.DELETE_ERROR) {
            final PutException putException = new PutException(
                    "Persistent storage returned unprocessable status code %s while deleting key %s",
                    storageStatus,
                    key
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
            throw putException;
        } else if (storageStatus == StatusType.DELETE_ERROR) {
            return new KVMessageImpl(key, StatusType.DELETE_ERROR);
        } else {
            final StatusType cacheStatus = cache.put(key, null).getStatus();
            if (cacheStatus != StatusType.DELETE_SUCCESS && cacheStatus != StatusType.DELETE_ERROR) {
                final PutException putException = new PutException(
                        "Cache storage returned unprocessable status code %s while deleting key %s",
                        storageStatus,
                        key
                );
                LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
                throw putException;
            }
            else return new KVMessageImpl(key, StatusType.DELETE_SUCCESS);
        }
    }

    @Override
    public synchronized KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        final KVMessage cacheResponse = cache.get(key);
        final StatusType cacheGetStatus = cacheResponse.getStatus();
        if (cacheGetStatus == StatusType.GET_SUCCESS) {
            LOGGER.debug("Found key {} with value {} in cache", key, cacheResponse.getValue());
            return cacheResponse;
        } else if (cacheGetStatus == StatusType.GET_ERROR) {
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

    /**
     * Tries to get a key from the storage after a cache miss
     */
    private KVMessage handleCacheMiss(String key) throws GetException {
        LOGGER.debug("Handling cache miss of key {}", key);
        try {
            final KVMessage storageResponse = persistentStorage.get(key);
            final StatusType storageStatus = storageResponse.getStatus();
            if (storageStatus == StatusType.GET_SUCCESS) {
                final String storageValue = storageResponse.getValue();
                LOGGER.debug("Found key {} with value {} in persistent storage", key, storageValue);
                return updateCache(key, storageValue);
            } else if (storageStatus == StatusType.GET_ERROR) {
                LOGGER.debug("Did not found key {} in persistent storage", key);
                return new KVMessageImpl(key, StatusType.GET_ERROR);
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

    /**
     * Updates the cache with a value from the storage after a cache miss
     */
    private KVMessage updateCache(String key, String storageValue) throws GetException {
        LOGGER.debug("Updating cache with key {} and value {}", key, storageValue);
        final KVMessage cachePutResponse = cache.put(key, storageValue);
        final StatusType cachePutStatus = cachePutResponse.getStatus();
        if (cachePutStatus == StatusType.PUT_SUCCESS) {
            LOGGER.debug("Successfully updated cache with key {} and value {}", key, storageValue);
            return new KVMessageImpl(key, storageValue, StatusType.GET_SUCCESS);
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
