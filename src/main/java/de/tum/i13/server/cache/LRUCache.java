package de.tum.i13.server.cache;

public class LRUCache extends AbstractLinkedHashMapCache {

    protected LRUCache(int size) {
        super(size, true);
    }

}
