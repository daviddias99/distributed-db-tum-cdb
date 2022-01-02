package de.tum.i13.server.kv.commandprocessing.handlers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

public class BulkReplicationHandler implements Runnable{
  
  private static final Logger LOGGER = LogManager.getLogger(BulkReplicationHandler.class);

  private final NetworkLocation peer;
  private final List<Pair<String>> elements;

  public BulkReplicationHandler(NetworkLocation peer, List<Pair<String>> elements) {
    this.peer = peer;
    this.elements = elements;
  }

  @Override
  public void run() {
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(), true);

    try {
      LOGGER.info("Trying to connect to peer {} for bulk replication", peer);
      netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
    } catch (CommunicationClientException e) {
      LOGGER.error("Could not connect to peer {} for bulk replication.", peer, e);
      return;
    }

    // Send items to peer
    for (Pair<String> item : elements) {
      try {
        LOGGER.info("Sending item with key {} to peer {}.", item.key, peer);
        KVMessage response = netPeerStorage.put(item.key, item.value);

        if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
          LOGGER.info("Sent item with key {} to peer {}.", item.key, peer);
        } else {
          LOGGER.error("Failed to send item with key {} to peer {}.", item.key, peer);
        }
      } catch (PutException e) {
        LOGGER.error("Could not send item with key {} to peer {}.", item.key, peer, e);
      }
    }
    
  }
}
