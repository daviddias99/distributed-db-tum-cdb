package de.tum.i13.client.net;

import org.apache.logging.log4j.LogManager;

/**
 * A {@link MessageServer} that is also a {@link NetworkConnection}
 */
public interface NetworkMessageServer extends NetworkConnection, MessageServer {

    /**
     * Connects client to {@code <address>:<port>} and returns the message sent by the host
     * upon connection. This method is equivalent to calling the {@link MessageServer#receive()} method
     * after the {@link NetworkConnection#connect(String, int)} method.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @return Bytes sent by the host.
     * @throws ClientException if the connecting or receiving fails
     * @see NetworkConnection#connect(String, int)
     * @see MessageServer#receive()
     */
    default byte[] connectAndReceive(String address, int port) throws ClientException {
        LogManager.getLogger(NetworkMessageServer.class).info("Connecting and receiving");
        this.connect(address, port);
        return this.receive();
    }

}
