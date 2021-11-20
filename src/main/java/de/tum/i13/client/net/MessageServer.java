package de.tum.i13.client.net;

public interface MessageServer {

    /**
     * Sends a message to the connected host. The message must be smaller then MAX_MESSAGE_SIZE_BYTES (see
     * 'Constants' class)
     *
     * @param message Message to be sent to the host.
     * @throws ClientException An exception is thrown when the message is too large, the client isn't connected or
     * the message fails to be sent.
     */
    void send(byte[] message) throws ClientException;

    /**
     * Receives a message from the host in the form of a byte array. Note that this method blocks waiting for data to
     * be sent.
     *
     * @return Bytes received from host.
     * @throws ClientException An exception is thrown if the client isn't connected or a message coundln't be received.
     */
    byte[] receive() throws ClientException;

}
