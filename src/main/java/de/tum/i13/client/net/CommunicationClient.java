package de.tum.i13.client.net;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

// TODO Use Autoclosable
public class CommunicationClient implements NetworkMessageServer {

    private static final int LOGGER_MAX_MESSAGE_PREVIEW_SIZE = 50;
    private static final Logger LOGGER = LogManager.getLogger(CommunicationClient.class);
    private Socket connection;
    private InputStream inStream;
    private OutputStream outStream;

    /**
     * Creates a new client. The created is not connected to any host.
     */
    public CommunicationClient() {
    }

    /**
     * Creates a new client connected to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @throws ClientException A ClientException is thrown when connection fails.
     *                         (see 'connect' method).
     */
    public CommunicationClient(String address, int port) throws ClientException {
        this.connect(address, port);
    }

    @Override
    public void connect(String address, int port) throws ClientException {
        LOGGER.info("Trying to connect to {}:{}", address, port);

        if (this.isConnected()) {
            this.disconnect();
        }

        LOGGER.info("Creating socket to {}:{}", address, port);

        try {
            // Open socket and get streams
            this.connection = new Socket(address, port);
            this.inStream = this.connection.getInputStream();
            this.outStream = this.connection.getOutputStream();
        } catch (UnknownHostException e) {
            LOGGER.error("Throwing exception because host {} could not be found.", address);
            throw new ClientException("Could not find host", ClientException.Type.UNKNOWN_HOST);
        } catch (IOException e) {
            LOGGER.error("Throwing exception because socket at {}:{} could not be opened.", address, port);
            throw new ClientException("Could not open socket", ClientException.Type.SOCKET_OPENING_ERROR);
        }
    }

    @Override
    public void disconnect() throws ClientException {
        LOGGER.info("Trying to disconnect from socket at {}:{}", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because a disconnection from a un-connected socket was made.");
            throw new ClientException("Cannot disconnect since a disconnect hasn't been made yet", ClientException.Type.UNCONNECTED);
        }

        try {
            LOGGER.debug("Closing connection from socket at {}:{}", getAddress(), getPort());
            this.connection.close();
            this.connection = null;
            this.inStream = null;
            this.outStream = null;
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error while closing connection/streams.");
            throw new ClientException("Error while closing client", ClientException.Type.SOCKET_CLOSING_ERROR);
        }
    }

    @Override
    public void send(byte[] message) throws ClientException {
        LOGGER.info("Trying to send message: {}", () -> new String(message));

        int messageSize = message.length;
        int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);

        LOGGER.info("Sending {} bytes to {}:{}. ({}{})",
                () -> messageSize, this::getAddress, this::getPort,
                () -> (new String(message)).substring(0, previewSize),
                () -> (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : ""));

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because data can't be send to an unconnected client.");
            throw new ClientException("No connection established", ClientException.Type.UNCONNECTED);
        }

        // Throw exception if message exceeds size
        if (message.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
            LOGGER.error("Throwing exception because data is to large (max is {} KB).", Constants.MAX_MESSAGE_SIZE_KB);
            throw new ClientException("Message too large", ClientException.Type.MESSAGE_TOO_LARGE);
        }

        try {
            // Write the whole array at once
            this.outStream.write(message);
            this.outStream.flush();
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error occurred while sending data.");
            throw new ClientException("Could not send message", ClientException.Type.INTERNAL_ERROR);
        }
    }

    @Override
    public byte[] receive() throws ClientException {
        LOGGER.info("Trying to receive message from {}:{}.", getAddress(), getPort());

        // Throw exception if no connection is open
        if (!this.isConnected()) {
            LOGGER.error("Throwing exception because data can't be send to an unconnected client.");
            throw new ClientException("No connection established", ClientException.Type.UNCONNECTED);
        }

        byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

        try {
            // Read all bytes at once
            int numberOfReceivedBytes = this.inStream.read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES);

            // Return an array with the size of the number of read bytes
            byte[] result = numberOfReceivedBytes == -1 ? new byte[0] : Arrays.copyOf(incomingMessageBuffer, numberOfReceivedBytes);

            // Logging aid variables
            int messageSize = result.length;
            int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);

            LOGGER.info("Receiving {} bytes to {}:{}. ({}{})",
                    () -> messageSize, this::getAddress, this::getPort,
                    () -> (new String(result)).substring(0, previewSize),
                    () -> (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : ""));
            return result;
        } catch (IOException e) {
            LOGGER.error("Throwing exception because an error occurred while receiving data.");
            throw new ClientException("Could not receive", ClientException.Type.INTERNAL_ERROR);
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

}