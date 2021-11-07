package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class AbstractLinkedHashMapCache implements Cache {

    private static final Logger LOGGER = LogManager.getLogger(AbstractLinkedHashMapCache.class);
    private final FixedSizeLinkedHashMap<String, String> cache;
    protected AbstractLinkedHashMapCache(int size, boolean accessOrder) {
        Preconditions.check(size > 0, "Cache must have a size greater than 0");

        cache = new FixedSizeLinkedHashMap<>(size, 0.75f, accessOrder);
    }

    @Override
    public KVMessage put(String key, String value) {
        Preconditions.notNull(key, "Key cannot be null");
        Preconditions.notNull(value, "Value cannot be null");
        LOGGER.info("Trying to put key {} with value {}", key, value);

        cache.put(key, value);
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
    }

    @Override
    public KVMessage get(String key) {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        return Optional.ofNullable(cache.get(key))
                .map(value -> new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS))
                .orElse(new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR));
    }

    private static class FixedSizeLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

        private static final Logger LOGGER = LogManager.getLogger(FixedSizeLinkedHashMap.class);
        private final int maxEntries;

        private FixedSizeLinkedHashMap(int maxEntries, float loadFactor, boolean accessOrder) {
            super(maxEntries, loadFactor, accessOrder);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            LOGGER.debug("Check for eldest entry");

            return this.size() > maxEntries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FixedSizeLinkedHashMap)) return false;
            if (!super.equals(o)) return false;
            FixedSizeLinkedHashMap<?, ?> that = (FixedSizeLinkedHashMap<?, ?>) o;
            return maxEntries == that.maxEntries;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), maxEntries);
        }

    }

}
