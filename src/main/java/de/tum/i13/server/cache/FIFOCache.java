package de.tum.i13.server.cache;

public class FIFOCache extends AbstractLinkedHashMapCache {

    protected FIFOCache(int size) {
        super(size, false);
    }

}
