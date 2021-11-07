package de.tum.i13.server.cache;

/**
 * A {@link Cache} with the {@link CachingStrategy} {@link CachingStrategy#LRU}
 */
public class LRUCache extends AbstractLinkedHashMapCache {

    /**
     * Constructs a cache with the given size
     *
     * @param size the size of the cache
     */
    protected LRUCache(int size) {
        super(size, true);
    }

    @Override
    public CachingStrategy getCachingStrategy() {
        return CachingStrategy.LRU;
    }

}
