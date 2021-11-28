package de.tum.i13.shared;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;

public interface CommandProcessor {

    String process(String command, PeerType peerType);

    String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);

    void connectionClosed(InetAddress address);
}
