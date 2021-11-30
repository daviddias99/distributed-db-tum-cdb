package de.tum.i13.shared;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;

public interface CommandProcessor {

    String process(String command, PeerType peerType);
}
