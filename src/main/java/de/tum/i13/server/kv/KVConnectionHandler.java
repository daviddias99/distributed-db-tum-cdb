package de.tum.i13.server.kv;

import de.tum.i13.shared.ConnectionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * An implementation of {@link ConnectionHandler}
 */
public class KVConnectionHandler implements ConnectionHandler {

    private static final Logger LOGGER = LogManager.getLogger(KVConnectionHandler.class);

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.debug("new connection: {}", remoteAddress);
        return "Connection to KVServer established: " + address.toString();
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        LOGGER.debug("connection closed: {}", remoteAddress);
    }

}
