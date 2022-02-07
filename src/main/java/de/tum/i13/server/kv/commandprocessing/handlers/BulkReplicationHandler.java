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
  private final boolean isDelete;

  public BulkReplicationHandler(NetworkLocation peer, List<Pair<String>> elements, boolean isDelete) {
    this.peer = peer;
    this.elements = elements;
    this.isDelete = isDelete;
  }

  public BulkReplicationHandler(NetworkLocation peer, List<Pair<String>> elements) {
    this(peer, elements, false);
  }

  @Override
  public void run() {
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(), WrappingPersistentStorage.MessageMode.SERVER);

    try {
      LOGGER.info("Trying to connect to peer {} for bulk replication", peer);
      netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
    } catch (CommunicationClientException e) {
      LOGGER.atError().withThrowable(e).log("Could not connect to peer {} for bulk replication.", peer);
      return;
    }

    // Send items to peer
    for (Pair<String> item : elements) {
      try {
        LOGGER.info("Sending item with key {} to peer {} (delete={}).", item.key, peer, this.isDelete);
        KVMessage response = netPeerStorage.put(item.key, this.isDelete ? null : item.value);

        if (response.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) || response.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS)) {
          LOGGER.info("{} item with key {} to peer {}.", this.isDelete ? "Delete" : "Sent", item.key, peer);
        } else {
          LOGGER.error("Failed to send item with key {} to peer {}  (delete={}).", item.key, peer, this.isDelete);
        }
      } catch (PutException e) {
        LOGGER.atError().withThrowable(e).log("Could not send item with key {} to peer {}.  (delete={})", item.key, peer, this.isDelete);
      }
    }

    LOGGER.info("Finished bulk replication to {}", peer);
  }
}
