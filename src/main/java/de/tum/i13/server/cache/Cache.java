package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;

public interface Cache extends KVStore {

    @Override
    KVMessage get(String key);

    @Override
    KVMessage put(String key, String value);

}
