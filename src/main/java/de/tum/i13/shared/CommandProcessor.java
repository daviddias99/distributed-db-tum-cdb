package de.tum.i13.shared;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;

public interface CommandProcessor<T> {

    T process(T command, PeerType peerType);
}
