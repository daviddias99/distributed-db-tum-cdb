package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;

public class KVStoreStub implements KVStore {

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO actual work on database
        return new KVMessageForStub(StatusType.PUT_SUCCESS, key, value);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // TODO actual work on database
        return new KVMessageForStub(StatusType.GET_SUCCESS, key, null);
    }
    
}
