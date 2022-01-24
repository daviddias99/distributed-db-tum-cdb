package de.tum.i13.server.kv.commandprocessing.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

/**
 * Handler that manages put and delete message replication to peer
 */
public class PutDeleteReplicationHandler implements Runnable {

  private static final Logger LOGGER = LogManager.getLogger(PutDeleteReplicationHandler.class);
  private String key;
  private String value;
  private NetworkLocation peer;

  /**
   * Create a new replication handler
   * 
   * @param peer                target server
   * @param key                 item key
   * @param value               item value (null if delete)
   */
  public PutDeleteReplicationHandler(NetworkLocation peer, String key, String value) {
    this.peer = peer;
    this.key = key;
    this.value = value;
  }

  @Override
  public void run() {
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(), true);
    String status = value == null ? "delete" : "put";
    try {
      LOGGER.info("Trying to connect to peer {} for replication", peer);
      netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
    } catch (CommunicationClientException e) {
      LOGGER.error("Could not connect to peer {} for replication.", peer, e);
    }
    // Send  message to peer
    try {
      LOGGER.info("Sending message {}({}) to peer {}.", status, key, peer);
      KVMessage response = netPeerStorage.put(this.key, this.value);

      if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
        LOGGER.info("Sent message {}({}) to peer {}.", status, key, peer);
      } else {
        LOGGER.info("Failed to send message {}({}) to peer {}.", status, key, peer);
      }
    } catch (PutException e) {
      LOGGER.info("Could not send send message {}({}) to peer {}.", status, key, peer);
    }
  }
}
