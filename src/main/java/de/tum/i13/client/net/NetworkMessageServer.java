package de.tum.i13.client.net;

public interface NetworkMessageServer extends NetworkLocation, MessageServer {

    /**
     * Connects client to {@code <address>:<port>} and returns the message sent by the host
     * upon connection. This method is equivalent to calling the 'receive' method
     * after the 'connect' method;
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @return Bytes sent by the host.
     * @throws ClientException
     */
    default byte[] connectAndReceive(String address, int port) throws ClientException {
        this.connect(address, port);
        return this.receive();
    }

}
