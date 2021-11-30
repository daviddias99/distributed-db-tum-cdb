package de.tum.i13.shared;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;

public interface CommandProcessor {

    String process(String command, PeerType peerType);


    // TODO: Should this be a responsability of a command processor?
    String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);
    
    // TODO: Should this be a responsability of a command processor?
    void connectionClosed(InetAddress address);
}
