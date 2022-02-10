package de.tum.i13.shared.net;

/**
 * A server to which the user can send messages and from which they can receive messages.
 */
public interface MessageServer {

    /**
     * Sends a message to the server.
     *
     * @param message Message to be sent to the host
     * @throws CommunicationClientException if the message is too large, the client isn't connected or
     *                                      the message fails to be sent
     */
    void send(String message) throws CommunicationClientException;

    /**
     * Receives a message from the server. Note that this method blocks waiting for data to
     * be sent.
     *
     * @return message received from server
     * @throws CommunicationClientException if the client isn't connected or a message couldn't be received
     */
    String receive() throws CommunicationClientException;

}
