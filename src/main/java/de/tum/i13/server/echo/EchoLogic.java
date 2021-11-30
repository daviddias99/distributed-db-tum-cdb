package de.tum.i13.server.echo;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class EchoLogic implements CommandProcessor<String>, ConnectionHandler {
    private static final Logger LOGGER = LogManager.getLogger(EchoLogic.class);

    public String process(String command, PeerType peerType) {
        LOGGER.info("received command: {}", command::trim);

        //Let the magic happen here

        return command;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: {}", remoteAddress);

        return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        LOGGER.info("connection closed: {}", remoteAddress);
    }
}
