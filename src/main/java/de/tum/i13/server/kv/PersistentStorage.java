package de.tum.i13.server.kv;

public interface PersistentStorage extends KVStore {

    @Override
    KVMessage get(String key) throws GetException;

    @Override
    KVMessage put(String key, String value) throws PutException;

}
