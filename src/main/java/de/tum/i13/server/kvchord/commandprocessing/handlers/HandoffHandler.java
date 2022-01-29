package de.tum.i13.server.kvchord.commandprocessing.handlers;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Handler that manages chunk handoff to other servers
 */
public class HandoffHandler implements Runnable {

  private final Logger LOGGER = LogManager.getLogger(HandoffHandler.class);

  private PersistentStorage storage;
  private NetworkLocation peer;
  private String lowerBound;
  private String upperBound;
  private List<String> nodesToDelete;
  private ChordServerState state;

  /**
   * Create a new handoff handler
   * @param peer target server
   * @param ecs ECS communications interface
   * @param lowerBound lower bound for the keys of the transfered elements
   * @param upperBound upper bound for the keys of the transfered elements
   * @param storage current server storage
   */
  public HandoffHandler(NetworkLocation peer, String lowerBound, String upperBound, PersistentStorage storage, ChordServerState state) {
    this.storage = storage;
    this.peer = peer;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.state = state;
    this.nodesToDelete = state.getDeleteQueue(); 
  }

  @Override
  public void run() {

    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(), true);

    try {
      LOGGER.info("Trying to connect to peer {} for handhoff", peer);
      netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
    } catch (CommunicationClientException e) {
      LOGGER.error("Could not connect to peer {} for handoff.", peer, e);
      return;
    }

    List<Pair<String>> itemsToSend = new LinkedList<>();

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

    this.state.executeStoredDeletes(storage);
  }

  private String padLeftZeros(String inputString, int length) {
    if (inputString.length() >= length) {
        return inputString;
    }
    StringBuilder sb = new StringBuilder();
    while (sb.length() < length - inputString.length()) {
        sb.append('0');
    }
    sb.append(inputString);

    return sb.toString();
}
  
  private List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {

    String paddedLower = this.padLeftZeros(lowerBound, 32);
    String paddedUpper = this.padLeftZeros(upperBound, 32);

    if(paddedLower.compareTo(paddedUpper) <= 0) {
      return this.storage.getRange(paddedLower, paddedUpper);
    }

    List<Pair<String>> result = new LinkedList<>();
    result.addAll(this.storage.getRange("00000000000000000000000000000000", paddedUpper));
    result.addAll(this.storage.getRange(paddedLower, "ffffffffffffffffffffffffffffffff"));

    return result;
  }
}
