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
        LOGGER.debug("Trying to connect to '{}:{}'", address, port);

        try {
            // Open socket and get streams
            LOGGER.trace("Creating socket to '{}:{}'", address, port);
            // Ignore the SonarLint warning because it is not aware that we close the socket elsewhere
            @SuppressWarnings("java:S2095") final Socket newConnection = new Socket(address, port);

            if (this.isConnected()) this.disconnect();

            this.connection = newConnection;
            this.inStream = this.connection.getInputStream();
            this.outStream = this.connection.getOutputStream();
        } catch (UnknownHostException e) {
            throw new CommunicationClientException(e, CommunicationClientException.Type.UNKNOWN_HOST, "Could not find" +
                    " host '%s'", address);
        } catch (IOException e) {
            throw new CommunicationClientException(e, CommunicationClientException.Type.CONNECTION_ERROR, "Could not " +
                    "open socket at '%s:%s'", address, port);
        }
    }

    @Override
    public void disconnect() throws CommunicationClientException {
        LOGGER.debug("Trying to disconnect from socket at '{}:{}'", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED,
                    "Cannot disconnect since no connection has been made yet");
        }

        try {
            LOGGER.trace("Closing connection from socket at '{}:{}'", getAddress(), getPort());
            this.connection.close();
            this.connection = null;
            this.inStream = null;
            this.outStream = null;
        } catch (IOException e) {
            throw new CommunicationClientException(e, CommunicationClientException.Type.SOCKET_CLOSING_ERROR, "Error " +
                    "while closing connection/streams");
        }
    }

    /**
     * {@inheritDoc} The message must be smaller than {@link  Constants#MAX_MESSAGE_SIZE_BYTES}
     *
     * @see Constants#MAX_MESSAGE_SIZE_BYTES
     */
    @Override
    public void send(String message) throws CommunicationClientException {
        LOGGER.debug("Trying to send message: '{}'", message);
        final String terminatedMessage = message + Constants.TERMINATING_STR;
        final byte[] bytes = message.getBytes(Constants.TELNET_ENCODING);
        final byte[] terminatedBytes = terminatedMessage.getBytes(Constants.TELNET_ENCODING);

        LOGGER.trace("Sending {} bytes to '{}:{}'. ('{}')",
                () -> terminatedBytes.length, this::getAddress, this::getPort,
                () -> message);

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED, "Data can't be send" +
                    " to an unconnected client.");
        }

        // Throw exception if message exceeds size
        if (bytes.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
            throw new CommunicationClientException(CommunicationClientException.Type.MESSAGE_TOO_LARGE, "Data is too " +
                    "large ('%s' bytes exceed maximum '%s' KB)", bytes.length, Constants.MAX_MESSAGE_SIZE_KB);
        }

        try {
            // Write the whole array at once
            this.outStream.write(terminatedBytes);
            this.outStream.flush();
        } catch (IOException e) {
            throw new CommunicationClientException(e, CommunicationClientException.Type.INTERNAL_ERROR, "Could not " +
                    "send message");
        }
    }

    @Override
    public String receive() throws CommunicationClientException {
        LOGGER.debug("Trying to receive message from '{}:{}'.", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            throw new CommunicationClientException(CommunicationClientException.Type.UNCONNECTED, "Data can't be " +
                    "received from an unconnected client");
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
            LOGGER.trace("Receiving {} bytes from '{}:{}'. ('{}')",
                    () -> result.length, this::getAddress, this::getPort,
                    () -> response);

            return response;
        } catch (IOException e) {
            throw new CommunicationClientException(e, CommunicationClientException.Type.INTERNAL_ERROR, "Could not " +
                    "receive data");
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