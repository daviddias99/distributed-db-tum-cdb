package de.tum.i13.client.net;

/**
 * A {@link NetworkLocation} that can be connected to.
 */
public interface NetworkConnection extends NetworkLocation {

    /**
     * Connects client to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination. Must be between 0 and 65535 inclusive.
     * @throws ClientException A ClientException is thrown when either the host.
     *                         address/port is invalid or a socket can't be created.
     */
    void connect(String address, int port) throws ClientException;

    /**
     * Disconnects from current connection.
     *
     * @throws ClientException An exception is thrown when the connection couldn't
     *                         been close or when disconnect is called on an
     *                         unconnected client.
     */
    void disconnect() throws ClientException;

    /**
     * This method returns true if the client is currently connected to a host.
     *
     * @return True if client is connected to host, false otherwise
     */
    boolean isConnected();

}
