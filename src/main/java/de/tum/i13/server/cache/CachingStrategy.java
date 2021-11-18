package de.tum.i13.server.cache;

/**
 * The different caching strategies a {@link Cache} can use.
 *
 * @see <a href="https://developpaper.com/three-cache-elimination-algorithms-lfu-lru-fifo/">Caching Stategies</a>
 */
public enum CachingStrategy {
    /**
     * Least Frequently Used strategy
     */
    LFU,
    /**
     * Least Recently Used strategy
     */
    LRU,
    /**
     * First In First Out strategy
     */
    FIFO
}
