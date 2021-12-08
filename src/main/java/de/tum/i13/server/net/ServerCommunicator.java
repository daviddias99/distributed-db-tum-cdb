package de.tum.i13.server.net;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkMessageServer;

public class ServerCommunicator implements NetworkMessageServer {
  private static final Logger LOGGER = LogManager.getLogger(ServerCommunicator.class);

  private final NetworkMessageServer networkMessageServer;

  private String lastAddress;
  private int lastPort = -1;

  /**
   * Communicator used for server-ecs communications
   *
   * @param networkMessageServer the server to use for network communication
   */
  public ServerCommunicator(NetworkMessageServer networkMessageServer) {
    this.networkMessageServer = networkMessageServer;
  }

  public KVMessage confirmHandoff() throws CommunicationClientException {
    LOGGER.info("Confirming handoff");
    KVMessage message = new KVMessageImpl(StatusType.SERVER_HANDOFF_SUCCESS);
    return this.sendAndReceive(message);
  }

  public KVMessage requestMetadata() throws CommunicationClientException {
    LOGGER.info("Requesting metadata");
    KVMessage message = new KVMessageImpl(StatusType.SERVER_GET_METADATA);
    return this.sendAndReceive(message);
  }

  public KVMessage sendError() throws CommunicationClientException {
    LOGGER.info("Sending error");
    KVMessage message = new KVMessageImpl(StatusType.ERROR);
    return this.sendAndReceive(message);
  }

  public KVMessage sendShutdown() throws CommunicationClientException {
    LOGGER.info("Sending shutdown");
    KVMessage message = new KVMessageImpl(StatusType.SERVER_SHUTDOWN);
    return this.sendAndReceive(message);
  }

  public KVMessage sendAndReceive(KVMessage message) throws CommunicationClientException {
    String packedMessage = message.packMessage();
    LOGGER.debug("Sending message to server: '{}'", packedMessage);
    networkMessageServer.send(packedMessage);
    try {
      LOGGER.debug("Receiving message from server");
      final String response = networkMessageServer.receive();
      LOGGER.debug("Received message from server: '{}'", response);
      return KVMessage.unpackMessage(response);
    } catch (IllegalArgumentException ex) {
      throw new CommunicationClientException(ex, "Could not unpack message received by the server");
    }
  }

  public void reconnect() throws CommunicationClientException {
    if (this.lastAddress == null || this.lastPort == -1) {
      throw new CommunicationClientException("Cannot reconnect to a port that has never been connected");
    }
    LOGGER.info("Trying to reconnect to {} {}", this.lastAddress, this.lastPort);
    
    this.connect(this.lastAddress, this.lastPort);
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
