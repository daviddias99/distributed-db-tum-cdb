package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.ActiveConnection;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

abstract class ECSThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ECSThread.class);

    private final Socket socket;
    private ActiveConnection activeConnection;

    /**
     * Handles a connection initiated from the server side. In this case no welcome message is consumed.
     *
     * @param socket the {@link Socket} where the connection was established
     * @throws IOException if the connection cannot be fully established
     */
    protected ECSThread(Socket socket) throws IOException {
        LOGGER.info("Creating new thread to communicate with server {}", socket);
        this.socket = socket;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Closing ECS connection to {}.", activeConnection);
            try {
                activeConnection.close();
            } catch (Exception ex) {
                LOGGER.atFatal().withThrowable(ex).log("Caught exception, while closing ECS socket", ex);
            }
        }));

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                Constants.TELNET_ENCODING));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Constants.TELNET_ENCODING));
        activeConnection = new ActiveConnection(socket, out, in);
    }

    /**
     * Initiates a connection from the ECS side. In this case the welcome message of the server is consumed.
     *
     * @param location the server to connect to
     * @throws IOException if the connection could not be established
     */
    protected ECSThread(NetworkLocation location) throws IOException {
        this(new Socket(location.getAddress(), location.getPort()));
        try {
            LOGGER.trace("Trying to receive welcome message");
            activeConnection.receive();
        } catch (CommunicationClientException exception) {
            throw new IOException("Could not receive welcome message", exception);
        }
    }

    /**
     * Sends a {@link KVMessage} message to the connected server and waits for a
     * response.
     * Checks the {@link KVMessage.StatusType} of the response against a provided expected
     * type.
     *
     * @param message      a {@link KVMessage} object containing the message to be
     *                     sent to server.
     * @param expectedType the {@link KVMessage.StatusType} object with the expecetd type of
     *                     the response {@link KVMessage}.
     * @throws IOException  if unable to read a response from the server.
     * @throws ECSException if communication is not as expected.
     */
    protected void sendAndReceiveMessage(KVMessage message, KVMessage.StatusType expectedType)
            throws IOException, ECSException {
        LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, message, expectedType);
        activeConnection.send(message.packMessage()); // send a message
        waitForResponse(expectedType);
    }

    /**
     * Method to wait for a response and determine if the response is as expected.
     *
     * @param expectedType The {@link KVMessage.StatusType} type that the
     *                     {@link KVMessage} response must have.
     * @return the {@link KVMessage} response received from the server.
     * @throws IOException  if unable to read from the connection.
     * @throws ECSException if the expected response is different from what is
     *                      received.
     */
    protected KVMessage waitForResponse(KVMessage.StatusType expectedType) throws IOException, ECSException {
        LOGGER.trace("Waiting for a message response");
        String response;
        try {
            response = activeConnection.receive();
        } catch (CommunicationClientException exception) {
            throw new IOException("Could not receive", exception);
        }

        final KVMessage unpackedResponse;
        try {
            unpackedResponse = KVMessage.unpackMessage(response);
        } catch (IllegalArgumentException exception) {
            throw new IOException("Could not unpack received message", exception);
        }

        if (response == null || "-1".equals(response)) {
            throw new ECSException(ECSException.Type.NO_ACK_RECEIVED,
                    "No response received from server");
        } else if (unpackedResponse.getStatus() != expectedType) {
            final ECSException exception = ECSException.determineException(expectedType);
            LOGGER.atError()
                    .withThrowable(exception)
                    .log("Message '{}' did not have expected status '{}'", unpackedResponse, expectedType);
            throw exception;
        }

        return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, unpackedResponse);
    }

    /**
     * Getter for the socket param
     *
     * @return the server connection socket
     */
    protected Socket getSocket() {
        return this.socket;
    }

    /**
     * Getter for {@link ActiveConnection} param
     *
     * @return
     */
    protected ActiveConnection getActiveConnection() {
        return this.activeConnection;
    }

}
