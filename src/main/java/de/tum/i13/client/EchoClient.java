package de.tum.i13.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;

public class EchoClient {

  private Socket connection;
  
  public EchoClient() {

  }

  public EchoClient(String address, int port) throws ClientException {
    this.connect(address, port);
  }

  public boolean connect(String address, int port) throws ClientException {
    try {
      Socket socket = new Socket(address, port);
      this.connection = socket;

      // Clear existing bytes from input
      this.receive();

    } catch (UnknownHostException e) {
      throw new ClientException("Could not find host");
    } catch (IOException e) {
      throw new ClientException("Could not open socket");
    }

    return true;
  }

  public void send(byte[] message) throws ClientException {

    if(connection == null) {
      throw new ClientException("No connection established");
    }
    
    if(message.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
      throw new ClientException("Message too large");
    }

    try {
      OutputStream outStream = this.connection.getOutputStream();
      outStream.write(message);
      outStream.flush();
    } catch (IOException e) {
      throw new ClientException("Could not send message");
    }
  }

  public byte[] receive() throws ClientException {

    byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

    try {
      int numberOfReceivedBytes = this.connection.getInputStream().read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES); 

      // Return an array with the size of the number of read bytes
      return Arrays.copyOf(incomingMessageBuffer, numberOfReceivedBytes);
    } catch (IOException e) {
      throw new ClientException("Could not receive");
    }
  }
}
