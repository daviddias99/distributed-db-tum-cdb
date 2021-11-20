package de.tum.i13.client.net;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;

import java.net.InetSocketAddress;
import java.util.Optional;

public class RemotePersistentStorage implements PersistentStorage {

    private InetSocketAddress inetSocketAddress;

    public RemotePersistentStorage() {
    }

    @Override
    public KVMessage get(String key) throws GetException {
        // TODO
        return null;
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        // TODO
        return null;
    }

    public Optional<InetSocketAddress> getInetSocketAddress() {
        return Optional.ofNullable(inetSocketAddress);
    }

}
