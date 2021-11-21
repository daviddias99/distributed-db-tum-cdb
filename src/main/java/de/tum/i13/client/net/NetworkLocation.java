package de.tum.i13.client.net;

/**
 * Represents a location somewhere on the network with a host address anda a port.
 */
public interface NetworkLocation {

    /**
     * Connects client to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @throws ClientException A ClientException is thrown when the either the host.
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

    /**
     * Returns the host address of the network location.
     *
     * @return the address of the network location
     */
    String getAddress();

    /**
     * Returns the port of the network location.
     *
     * @return the port of the network location
     */
    int getPort();

}
