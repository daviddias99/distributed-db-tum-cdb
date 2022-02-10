package de.tum.i13.shared;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * TODO: This interface should either change name or be merged with a more
 * appropriate one. Changed this here because it seemed that this shouldn't be a
 * responsability for a command processor
 * TODO: check comments in methods
 */
public interface ConnectionHandler {

    /**
     * Return message to be sent on connection accepted
     *
     * @param address       server address
     * @param remoteAddress remote peer address
     * @return message to be sent on connection accepted
     */
    String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);

    /**
     * Handler for closed connection
     *
     * @param address server address
     */
    void connectionClosed(InetAddress address);

}
