package de.tum.i13.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;

public class EchoClient {

  private Socket connection;
  private InputStream inStream;
  private OutputStream outStream;
  
  public EchoClient() {

  }

  public EchoClient(String address, int port) throws ClientException {
    this.connect(address, port);
  }

  public void connect(String address, int port) throws ClientException {
    try {

      // Open socket and get streams
      Socket socket = new Socket(address, port);
      this.connection = socket;
      this.inStream= this.connection.getInputStream();
      this.outStream = this.connection.getOutputStream();

      // Clear existing bytes from input
      this.receive();

    } catch (UnknownHostException e) {
      this.disconnect();
      throw new ClientException("Could not find host");
    } catch (IOException e) {
      this.disconnect();
      throw new ClientException("Could not open socket");
    }
  }

  public void disconnect() throws ClientException {
    try {
      this.inStream.close();
      this.outStream.close();
      this.connection.close();
    } catch (IOException e) {
      throw new ClientException("Error while closing client");
    }
  }

  public void send(byte[] message) throws ClientException {

    if(connection == null) {
      throw new ClientException("No connection established");
    }
    
    if(message.length > Constants.MAX_MESSAGE_SIZE_BYTES) {
      throw new ClientException("Message too large");
    }

    try {
      this.outStream.write(message);
      this.outStream.flush();
    } catch (IOException e) {
      throw new ClientException("Could not send message");
    }
  }

  public byte[] receive() throws ClientException {

    byte[] incomingMessageBuffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

    try {
      int numberOfReceivedBytes = this.inStream.read(incomingMessageBuffer, 0, Constants.MAX_MESSAGE_SIZE_BYTES); 

      // Return an array with the size of the number of read bytes
      return Arrays.copyOf(incomingMessageBuffer, numberOfReceivedBytes);
    } catch (IOException e) {
      throw new ClientException("Could not receive");
    }
  }

  public static void main( String[] args){
      System.out.println("Echo");
  }
}
