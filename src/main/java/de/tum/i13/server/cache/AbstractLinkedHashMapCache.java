package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class AbstractLinkedHashMapCache implements KVStore {

    private static class FixedSizeLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

        private final int maxEntries;

        private FixedSizeLinkedHashMap(int maxEntries, float loadFactor, boolean accessOrder) {
            super(maxEntries, loadFactor, accessOrder);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
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

    private final FixedSizeLinkedHashMap<String, String> cache;

    protected AbstractLinkedHashMapCache(int size, boolean accessOrder) {
        cache = new FixedSizeLinkedHashMap<>(size, 0.75f, accessOrder);
    }

    @Override
    public KVMessage put(String key, String value) {
        Preconditions.notNull(key);
        Preconditions.notNull(value);
        cache.put(key, value);
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
    }

    @Override
    public KVMessage get(String key) {
        Preconditions.notNull(key);
        final Optional<String> optionalValue = Optional.ofNullable(cache.get(key));
        return optionalValue
                .map(value -> new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS))
                .orElseGet(() -> new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR));
    }

}
