package de.tum.i13.shared.persistentStorage;

import de.tum.i13.shared.net.NetworkMessageServer;

/**
 * A {@link PersistentStorage} that is also a {@link NetworkMessageServer}.
 */
public interface NetworkPersistentStorage extends PersistentStorage, NetworkMessageServer {

}
