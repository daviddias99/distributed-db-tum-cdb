package de.tum.i13.server.net;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.NetworkMessageServer;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.Constants;

public class ServerCommunicator implements NetworkMessageServer {

  private final NetworkMessageServer networkMessageServer;

  /**
   * Creates a new {@link WrappingPersistentStorage} that wraps around the given
   * {@link NetworkMessageServer}
   *
   * @param networkMessageServer the server to use for network communication
   */
  public ServerCommunicator(NetworkMessageServer networkMessageServer) {
    this.networkMessageServer = networkMessageServer;
  }

  public KVMessage confirmHandoff() throws ClientException {
    KVMessage message = new KVMessageImpl(StatusType.SERVER_HANDOFF_SUCCESS);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage.getBytes(Constants.TELNET_ENCODING));

    try {
      return KVMessage.unpackMessage(receiveMessage());
    } catch (IllegalArgumentException ex) {
      throw new ClientException(ex, "Could not unpack message received by the server");
    }
  }

  public KVMessage requestMetadata() throws ClientException {
    KVMessage message = new KVMessageImpl(StatusType.SERVER_GET_METADATA);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage.getBytes(Constants.TELNET_ENCODING));

    try {
      return KVMessage.unpackMessage(receiveMessage());
    } catch (IllegalArgumentException ex) {
      throw new ClientException(ex, "Could not unpack message received by the server");
    }
  }

  private String receiveMessage() throws ClientException {
    byte[] response = networkMessageServer.receive();
    final String responseString = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
    return responseString;
}

  @Override
  public void send(byte[] message) throws ClientException {
    networkMessageServer.send(message);
  }

  @Override
  public byte[] receive() throws ClientException {
    return networkMessageServer.receive();
  }

  @Override
  public void connect(String address, int port) throws ClientException {
    networkMessageServer.connect(address, port);
  }

  @Override
  public void disconnect() throws ClientException {
    networkMessageServer.disconnect();
  }

  @Override
  public boolean isConnected() {
    return networkMessageServer.isConnected();
  }

  @Override
  public String getAddress() {
    return networkMessageServer.getAddress();
  }

  @Override
  public int getPort() {
    return networkMessageServer.getPort();
  }

}
