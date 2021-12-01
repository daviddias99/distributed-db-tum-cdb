package de.tum.i13.client.net;

import de.tum.i13.server.persistentstorage.PersistentStorage;

/**
 * A {@link PersistentStorage} that is also a {@link NetworkMessageServer}.
 */
public interface NetworkPersistentStorage extends PersistentStorage, NetworkMessageServer {

}
