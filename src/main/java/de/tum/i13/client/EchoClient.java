package de.tum.i13.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.Logger;

import de.tum.i13.client.exceptions.ClientException.ClientException;
import de.tum.i13.shared.Constants;

public class EchoClient {

  private Socket connection;
  private InputStream inStream;
  private OutputStream outStream;

  private String address;
  private int port;

  private final int LOGGER_MAX_MESSAGE_PREVIEW_SIZE = 50;
  private final static Logger LOGGER = Logger.getLogger(TestClient.class.getName());

  public EchoClient() {

  }

  public EchoClient(String address, int port) throws ClientException {
    this.connect(address, port);
  }

  public byte[] connect(String address, int port) throws ClientException {
    return this.connect(address, port, true);
  }

  public byte[] connect(String address, int port, boolean returnInitialData) throws ClientException {
    this.address = address;
    this.port = port;
    LOGGER.info(String.format("Creating socket to %s:%d", address, port));

    try {
      // Open socket and get streams
      Socket socket = new Socket(address, port);
      this.connection = socket;
      this.inStream = this.connection.getInputStream();
      this.outStream = this.connection.getOutputStream();

      return returnInitialData ? this.receive() : new byte[0];
    } catch (UnknownHostException e) {
      this.disconnect();
      LOGGER.severe(String.format("Throwing exception because host (%s) could not be found."));
      throw new ClientException("Could not find host");
    } catch (IOException e) {
      this.disconnect();
      LOGGER.severe(String.format("Throwing exception because socket at %s:%d not be found.", address, port));
      throw new ClientException("Could not open socket");
    }
  }

  public void disconnect() throws ClientException {
    LOGGER.info(String.format("Disconnecting from socket at %s:%d", address, port));

    // Throw exception if no connection is open
    if (this.connection == null) {
      LOGGER.severe("Throwing exception because a disconnection from a un-connected socket was made.");
      throw new ClientException("Cannot disconnect since a disconnect hasn't been made yet");
    }

    try {
      LOGGER.fine(String.format("Closing input stream from socket at %s:%d", address, port));
      this.inStream.close();
      LOGGER.fine(String.format("Closing output stream from socket at %s:%d", address, port));
      this.outStream.close();
      LOGGER.fine(String.format("Closing connection from socket at %s:%d", address, port));
      this.connection.close();
    } catch (IOException e) {
      LOGGER.severe("Throwing exception because an error while closing connection/streams.");
      throw new ClientException("Error while closing client");
    }
  }

  public void send(byte[] message) throws ClientException {

    int messageSize = message.length;
    int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);

    LOGGER.info(String.format("Sending %d bytes to %s:%d. (%s%s)", messageSize, address, port,
        (new String(message)).substring(0, previewSize),
        (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : "")));

    // Throw exception if no connection is open
    if (connection == null) {
      LOGGER.severe("Throwing exception because data can't be send to an unconnected client.");
      throw new ClientException("No connection established");
    }

    // Throw exception if message exceeds size
    if (message.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
      LOGGER.severe(
          String.format("Throwing exception because data is to large (max is %s KB).", Constants.MAX_MESSAGE_SIZE_KB));
      throw new ClientException("Message too large");
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
      LOGGER.severe(String.format("Throwing exception because an error occured while sending data.",
          Constants.MAX_MESSAGE_SIZE_KB));
      throw new ClientException("Could not send message");
    }
  }

  public byte[] receive() throws ClientException {

    LOGGER.info(String.format("Receiving message from %s:%d.", address, port));

    // Throw exception if no connection is open
    if (connection == null) {
      LOGGER.severe("Throwing exception because data can't be send to an unconnected client.");
      throw new ClientException("No connection established");
    }

    byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

    try {
      // Read all bytes at once
      int numberOfReceivedBytes = this.inStream.read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES);

      // Return an array with the size of the number of read bytes
      byte[] result = Arrays.copyOf(incomingMessageBuffer, numberOfReceivedBytes);
      
      // Logging aid variables
      int messageSize = result.length;
      int previewSize = Math.min(messageSize, LOGGER_MAX_MESSAGE_PREVIEW_SIZE);
      
      LOGGER.info(String.format("Receiving %d bytes to %s:%d. (%s%s)", messageSize, address, port,
          (new String(result)).substring(0, previewSize),
          (previewSize == LOGGER_MAX_MESSAGE_PREVIEW_SIZE ? "..." : "")));
        
      return result;
    } catch (IOException e) {
      LOGGER.severe("Throwing exception because an error occured while receiving data.");
      throw new ClientException("Could not receive");
    }
  }
}
