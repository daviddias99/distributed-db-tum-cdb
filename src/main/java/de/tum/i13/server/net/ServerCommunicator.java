package de.tum.i13.server.net;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkMessageServer;

public class ServerCommunicator implements NetworkMessageServer {

  private final NetworkMessageServer networkMessageServer;

  private String lastAddress;
  private int lastPort = -1;

  /**
   * Creates a new {@link WrappingPersistentStorage} that wraps around the given
   * {@link NetworkMessageServer}
   *
   * @param networkMessageServer the server to use for network communication
   */
  public ServerCommunicator(NetworkMessageServer networkMessageServer) {
    this.networkMessageServer = networkMessageServer;
  }

  public KVMessage confirmHandoff() throws CommunicationClientException {
    KVMessage message = new KVMessageImpl(StatusType.SERVER_HANDOFF_SUCCESS);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage);

    try {
      return KVMessage.unpackMessage(receive());
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  public KVMessage requestMetadata() throws CommunicationClientException {
    KVMessage message = new KVMessageImpl(StatusType.SERVER_GET_METADATA);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage);

    try {
      return KVMessage.unpackMessage(receive());
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  public KVMessage sendError() throws CommunicationClientException {
    KVMessage message = new KVMessageImpl(StatusType.ERROR);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage);

    try {
      return KVMessage.unpackMessage(receive());
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  public KVMessage sendShutdown() throws CommunicationClientException {
    KVMessage message = new KVMessageImpl(StatusType.SERVER_SHUTDOWN);
    final String terminatedMessage = message.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage);

    try {
      return KVMessage.unpackMessage(receive());
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  public KVMessage sendMessage(KVMessage outgoingMessage) throws CommunicationClientException {
    final String terminatedMessage = outgoingMessage.packMessage() + Constants.TERMINATING_STR;
    networkMessageServer.send(terminatedMessage);

    try {
      return KVMessage.unpackMessage(receive());
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  @Override
  public synchronized void send(String message) throws CommunicationClientException {
    networkMessageServer.send(message);
  }

  @Override
  public synchronized String receive() throws CommunicationClientException {
    return networkMessageServer.receive();
  }

  @Override
  public void connect(String address, int port) throws CommunicationClientException {
    this.lastAddress = address;
    this.lastPort = port;
    networkMessageServer.connect(address, port);
  }

  public void reconnect() throws CommunicationClientException {

    if (this.lastAddress == null || this.lastPort == -1) {
      throw new CommunicationClientException("Cannot reconnect to a port that has never been connected");
    }

    this.connect(this.lastAddress, this.lastPort);
  }

  @Override
  public void disconnect() throws CommunicationClientException {
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
