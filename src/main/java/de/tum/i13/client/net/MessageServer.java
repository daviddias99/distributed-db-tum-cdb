package de.tum.i13.client.net;

import de.tum.i13.shared.Constants;

/**
 * A server to which the user can send messages and from which they can receive messages.
 */
public interface MessageServer {

    /**
     * Sends a message to the connected host. The message must be smaller than {@link  Constants#MAX_MESSAGE_SIZE_BYTES}
     *
     * @param message Message to be sent to the host
     * @throws ClientException if the message is too large, the client isn't connected or
     * the message fails to be sent
     * @see Constants#MAX_MESSAGE_SIZE_BYTES
     */
    void send(byte[] message) throws ClientException;

    /**
     * Receives a message from the server in the form of a byte array. Note that this method blocks waiting for data to
     * be sent.
     *
     * @return Bytes received from server
     * @throws ClientException if the client isn't connected or a message couldn't be received
     */
    byte[] receive() throws ClientException;

}
