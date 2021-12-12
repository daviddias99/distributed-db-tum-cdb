package de.tum.i13.server.kv.commandprocessing.handlers;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

/**
 * Handler that manages chunk handoff to other servers
 */
public class HandoffHandler implements Runnable {

  private static final Logger LOGGER = LogManager.getLogger(HandoffHandler.class);

  private PersistentStorage storage;
  private ServerCommunicator ecs;
  private NetworkLocation peer;
  private String lowerBound;
  private String upperBound;

  /**
   * Create a new handoff handler
   * @param peer target server
   * @param ecs ECS communications interface
   * @param lowerBound lower bound for the keys of the transfered elements
   * @param upperBound upper bound for the keys of the transfered elements
   * @param storage current server storage
   */
  public HandoffHandler(NetworkLocation peer, ServerCommunicator ecs, String lowerBound, String upperBound,
      PersistentStorage storage) {
    this.storage = storage;
    this.peer = peer;
    this.ecs = ecs;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  public void run() {
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(), true);

    try {
      LOGGER.info("Trying to connect to peer {} for handhoff", peer);
      netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
    } catch (CommunicationClientException e) {
      LOGGER.error("Could not connect to peer {} for handoff.", peer, e);
    }

    List<Pair<String>> itemsToSend = new LinkedList<>();
    List<String> nodesToDelete = new LinkedList<>();

    // Fetch items from storage
    try {
      LOGGER.info("Fetching range from database");
      itemsToSend = this.getRange(lowerBound, upperBound);
    } catch (GetException e) {
      LOGGER.error("Error while getting key range during handoff.", e);
    }

    // Send items to peer
    for (Pair<String> item : itemsToSend) {
      try {
        LOGGER.info("Sending item with key {} to peer {}.", item.key, peer);
        KVMessage response = netPeerStorage.put(item.key, item.value);

        if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
          LOGGER.info("Sent item with key {} to peer {}.", item.key, peer);
          nodesToDelete.add(item.key);
        } else {
          LOGGER.error("Failed to send item with key {} to peer {}.", item.key, peer);
        }
      } catch (PutException e) {
        LOGGER.error("Could not send item with key {} to peer {}.", item.key, peer, e);
      }
    }

    // Delete items after sending (only sucessful ones)
    for (String key : nodesToDelete) {
      try {
        LOGGER.info("Trying to delete item with key {}.", key);
        storage.put(key, null);
      } catch (PutException e) {
        LOGGER.error("Could not delete item with key {} during handoff to {}.", key, peer, e);
      }
    }
  }
  
  private List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {
    if(lowerBound.compareTo(upperBound) <= 0) {
      return this.storage.getRange(lowerBound, upperBound);
    }

    List<Pair<String>> result = new LinkedList<>();
    result.addAll(this.storage.getRange("00000000000000000000000000000000", upperBound));
    result.addAll(this.storage.getRange(lowerBound, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));

    return result;
  }
}
