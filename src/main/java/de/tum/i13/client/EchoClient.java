package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException.ClientException;
import de.tum.i13.client.exceptions.ClientException.ClientExceptionType;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.client.exceptions.ClientExceptionType;
import de.tum.i13.shared.Constants;

public class EchoClient {

    private Socket connection;
    private InputStream inStream;
    private OutputStream outStream;

    private String address;
    private int port;

  private static final int LOGGER_MAX_MESSAGE_PREVIEW_SIZE = 50;
  private static final Logger LOGGER = LogManager.getLogger(EchoClient.class);

  /**
   * Creates a new client. The created is not connected to any host.
   */
  public EchoClient() {
  }

    /**
     * Creates a new client connected to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @throws ClientException A ClientException is thrown when connection fails.
     *                         (see 'connect' method).
     */
    public EchoClient(String address, int port) throws ClientException {
        this.connect(address, port);
    }

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
    public byte[] connectAndReceive(String address, int port) throws ClientException {
        this.connect(address, port);
        return this.receive();
    }

    /**
     * Connects client to {@code <address>:<port>}.
     *
     * @param address Hostname or address of the destination.
     * @param port    Port of the destination.
     * @throws ClientException A ClientException is thrown when the either the host.
     *                         address/port is invalid or a socket can't be created.
     */
    public void connect(String address, int port) throws ClientException {

        if (this.isConnected()) {
            this.disconnect();
        }

    this.address = address;
    this.port = port;
    LOGGER.info("Creating socket to {}:{}", address, port);

    try {
      // Open socket and get streams
      Socket socket = new Socket(address, port);
      this.connection = socket;
      this.inStream = this.connection.getInputStream();
      this.outStream = this.connection.getOutputStream();
    } catch (UnknownHostException e) {
      LOGGER.error("Throwing exception because host {} could not be found.", address);
      throw new ClientException("Could not find host", ClientExceptionType.UNKNOWN_HOST);
    } catch (IOException e) {
      LOGGER.error("Throwing exception because socket at {}:{} could not be opened.", address, port);
      throw new ClientException("Could not open socket", ClientExceptionType.SOCKET_OPENING_ERROR);
    }

  /**
   * Disconnects from current connection.
   * 
   * @throws ClientException An exception is thrown when the connection couldn't
   *                         been close or when disconnect is called on an
   *                         unconnected client.
   */
  public void disconnect() throws ClientException {
    LOGGER.info("Disconnecting from socket at {}:{}", address, port);

    // Throw exception if no connection is open
    if (!this.isConnected()) {
      LOGGER.error("Throwing exception because a disconnection from a un-connected socket was made.");
      throw new ClientException("Cannot disconnect since a disconnect hasn't been made yet", ClientExceptionType.UNCONNECTED);
    }

    try {
      LOGGER.debug("Closing connection from socket at {}:{}", address, port);
      this.connection.close();
    } catch (IOException e) {
      LOGGER.error("Throwing exception because an error while closing connection/streams.");
      throw new ClientException("Error while closing client", ClientExceptionType.SOCKET_CLOSING_ERROR);
    }

    /**
     * Sends a message to the connected host. The message must be smaller then MAX_MESSAGE_SIZE_BYTES (see 'Constants' class)
     * @param message  Message to be sent to the host.
     * @throws ClientException  An exception is thrown when the message is too large, the client isn't connected or the message fails to be sent.
     */
    public void send(byte[] message) throws ClientException {

        int messageSize = message.length;
        int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);

    LOGGER.info("Sending {} bytes to {}:{}. ({}{})",
            () -> messageSize, () -> address, () -> port,
            () -> (new String(message)).substring(0, previewSize),
            () -> (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : ""));

    // Throw exception if no connection is open
    if (!this.isConnected()) {
      LOGGER.error("Throwing exception because data can't be send to an unconnected client.");
      throw new ClientException("No connection established", ClientExceptionType.UNCONNECTED);
    }

    // Throw exception if message exceeds size
    if (message.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
      LOGGER.error("Throwing exception because data is to large (max is {} KB).", Constants.MAX_MESSAGE_SIZE_KB);
      throw new ClientException("Message too large", ClientExceptionType.MESSAGE_TOO_LARGE);
    }

        try {
            // Write the whole array at once
            this.outStream.write(message);

            /*
             * // Write one byte at a time. for (byte b : message) { * for (byte b :
             * message) { this.outStream.write(b); }
             */

            this.outStream.flush();
        } catch (

    IOException e) {
      LOGGER.error("Throwing exception because an error occured while sending data.");
      throw new ClientException("Could not send message", ClientExceptionType.INTERNAL_ERROR);
    }

    /**
     * Receives a message from the host in the form of a byte array. Note that this method blocks waiting for data to be sent.
     * @return Bytes received from host.
     * @throws ClientException An exception is thrown if the client isn't connected or a message coundln't be received.
     */
    public byte[] receive() throws ClientException {

    LOGGER.info("Receiving message from {}:{}.", address, port);

    // Throw exception if no connection is open
    if (!this.isConnected()) {
      LOGGER.error("Throwing exception because data can't be send to an unconnected client.");
      throw new ClientException("No connection established", ClientExceptionType.UNCONNECTED);
    }

        byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

        try {
            // Read all bytes at once
            int numberOfReceivedBytes = this.inStream.read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES);

            // Return an array with the size of the number of read bytes
            byte[] result = numberOfReceivedBytes == -1 ? new byte[0]: Arrays.copyOf(incomingMessageBuffer, numberOfReceivedBytes);

            // Logging aid variables
            int messageSize = result.length;
            int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);

      LOGGER.info("Receiving {} bytes to {}:{}. ({}{})",
              () -> messageSize, () -> address, () -> port,
              () ->(new String(result)).substring(0, previewSize),
              () -> (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : ""));
      return result;
    } catch (IOException e) {
      LOGGER.error("Throwing exception because an error occured while receiving data.");
      throw new ClientException("Could not receive", ClientExceptionType.INTERNAL_ERROR);
    }

    /**
     * This method returns true if the client is currently connected to a host.
     * @return  True if client is connected to host, false otherwise
     */
    public boolean isConnected() {
        return this.connection != null && !this.connection.isClosed();
    }
}
