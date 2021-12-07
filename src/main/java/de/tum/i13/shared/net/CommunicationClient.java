package de.tum.i13.shared.net;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * A specific implementation of a {@link NetworkMessageServer} using {@link Socket}s.
 */
public class CommunicationClient implements NetworkMessageServer, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(CommunicationClient.class);
    private Socket connection;
    private InputStream inStream;
    private OutputStream outStream;

    /**
     * Creates a new client that is not connected to any host.
     */
    public CommunicationClient() {
    }

    /**
     * Creates a new client connected to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination. Must be between 0 and 65535 inclusive.
     * @throws CommunicationClientException if connection fails
     * @see NetworkConnection#connect(String, int)
     */
    public CommunicationClient(String address, int port) throws CommunicationClientException {
        this.connect(address, port);
    }

    @Override
    public void connect(String address, int port) throws CommunicationClientException {
        Preconditions.check(port >= 0 && port <= 65535, "Port must be between 0 and 65535 inclusive");
        LOGGER.info("Trying to connect to '{}:{}'", address, port);

        if (this.isConnected()) {
            this.disconnect();
        }

        LOGGER.info("Creating socket to '{}:{}'", address, port);

        try {
            // Open socket and get streams
            this.connection = new Socket(address, port);
            this.inStream = this.connection.getInputStream();
            this.outStream = this.connection.getOutputStream();
        } catch (UnknownHostException e) {
            LOGGER.error("Throwing exception because host '{}' could not be found.", address);
            throw new CommunicationClientException(e, CommunicationClientException.Type.UNKNOWN_HOST, "Could not find host");
        } catch (IOException e) {
            LOGGER.error("Throwing exception because socket at '{}:{}' could not be opened.", address, port);
            throw new CommunicationClientException(e, CommunicationClientException.Type.CONNECTION_ERROR, "Could not open socket");
        }
    }

    @Override
    public void disconnect() throws CommunicationClientException {
        LOGGER.info("Trying to disconnect from socket at '{}:{}'", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because a disconnection from a un-connected socket was made.");
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED,
                    "Cannot disconnect since a disconnect hasn't been made yet");
        }

        try {
            LOGGER.debug("Closing connection from socket at '{}:{}'", getAddress(), getPort());
            this.connection.close();
            this.connection = null;
            this.inStream = null;
            this.outStream = null;
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error while closing connection/streams.");
            throw new CommunicationClientException(e, CommunicationClientException.Type.SOCKET_CLOSING_ERROR, "Error while closing client");
        }
    }

    /**
     * {@inheritDoc} The message must be smaller than {@link  Constants#MAX_MESSAGE_SIZE_BYTES}
     *
     * @see Constants#MAX_MESSAGE_SIZE_BYTES
     */
    @Override
    public void send(String message) throws CommunicationClientException {
        LOGGER.info("Trying to send message: '{}'", message);
        final String terminatedMessage = message + Constants.TERMINATING_STR;
        final byte[] bytes = message.getBytes(Constants.TELNET_ENCODING);
        final byte[] terminatedBytes = terminatedMessage.getBytes(Constants.TELNET_ENCODING);

        LOGGER.info("Sending {} bytes to '{}:{}'. ('{}')",
                () -> terminatedBytes.length, this::getAddress, this::getPort,
                () -> message);

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because data can't be send to an unconnected client.");
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED, "No connection established");
        }

        // Throw exception if message exceeds size
        if (bytes.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
            LOGGER.error("Throwing exception because data is to large (max is {} KB).", Constants.MAX_MESSAGE_SIZE_KB);
            throw new CommunicationClientException(CommunicationClientException.Type.MESSAGE_TOO_LARGE, "Message too large");
        }

        try {
            // Write the whole array at once
            this.outStream.write(terminatedBytes);
            this.outStream.flush();
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error occurred while sending data.");
            throw new CommunicationClientException(e, CommunicationClientException.Type.INTERNAL_ERROR, "Could not send message");
        }
    }

    @Override
    public String receive() throws CommunicationClientException {
        LOGGER.info("Trying to receive message from '{}:{}'.", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because data can't be received from an unconnected client.");
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED, "No connection established");
        }

        byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

        try {
            // Read all bytes at once
            int numberOfReceivedBytes = this.inStream.read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES);

            // Return an array with the size of the number of read bytes
            byte[] result = numberOfReceivedBytes == -1 ? new byte[0] : Arrays.copyOf(incomingMessageBuffer,
                    numberOfReceivedBytes);
            String response = new String(result, 0, result.length - 2, Constants.TELNET_ENCODING);

            // Logging aid variables
            LOGGER.info("Receiving {} bytes from '{}:{}'. ('{}')",
                    () -> result.length, this::getAddress, this::getPort,
                    () -> response);

            return response;
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error occurred while receiving data.");
            throw new CommunicationClientException(e, CommunicationClientException.Type.INTERNAL_ERROR, "Could not receive");
        }
    }

    @Override
    public boolean isConnected() {
        return this.connection != null && !this.connection.isClosed();
    }

    @Override
    public String getAddress() {
        return isConnected() ? this.connection.getInetAddress().getHostName() : null;
    }

    @Override
    public int getPort() {
        return isConnected() ? this.connection.getPort() : -1;
    }

    @Override
    public void close() throws CommunicationClientException {
        disconnect();
    }

}