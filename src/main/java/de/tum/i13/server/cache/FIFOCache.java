package de.tum.i13.server.cache;

/**
 * A {@link Cache} with the {@link CachingStrategy} {@link CachingStrategy#FIFO}
 */
public class FIFOCache extends AbstractLinkedHashMapCache {

    /**
     * Constructs a cache with the given size
     *
     * @param size the size of the cache
     */
    protected FIFOCache(int size) {
        super(size, false);
    }

    @Override
    public CachingStrategy getCachingStrategy() {
        return CachingStrategy.FIFO;
    }

}
