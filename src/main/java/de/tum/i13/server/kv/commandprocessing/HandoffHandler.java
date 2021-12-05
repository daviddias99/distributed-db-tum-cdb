package de.tum.i13.server.kv.commandprocessing;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.client.net.NetworkPersistentStorage;
import de.tum.i13.client.net.WrappingPersistentStorage;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.GetException;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.persistentstorage.PutException;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.NetworkLocation;

public class HandoffHandler implements Runnable {

  private static final Logger LOGGER = LogManager.getLogger(HandoffHandler.class);

  private PersistentStorage storage;
  private ServerCommunicator ecs;
  private NetworkLocation peer;
  private String lowerBound;
  private String upperBound;

  public HandoffHandler(NetworkLocation peer, ServerCommunicator ecs, String lowerBound, String upperBound,
      PersistentStorage storage) {
    this.storage = storage;
    this.peer = peer;
    this.ecs = ecs;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public void run() {
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient());

    try {
      netPeerStorage.connect(peer.getAddress(), peer.getPort());
    } catch (ClientException e) {
      LOGGER.error("Could not connect to peer {} for handoff.", peer, e);
    }

    List<Pair<String>> itemsToSend = new LinkedList<>();
    List<String> nodesToDelete = new LinkedList<>();

    // Fetch items from storage
    try {
      itemsToSend = this.storage.getRange(lowerBound, upperBound);
    } catch (GetException e) {
      LOGGER.error("Error while getting key range during handoff.", e);
    }

    // Send items to peer
    for (Pair<String> item : itemsToSend) {
      try {

        KVMessage response = netPeerStorage.put(item.key, item.value);

        if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
          nodesToDelete.add(item.key);
        }
      } catch (PutException e) {
        LOGGER.error("Could not send item with key {} to peer {}.", item.key, peer, e);
      }
    }

    // Delete items after sending (only sucessful ones)
    for (String key : nodesToDelete) {
      try {
        storage.put(key, null);
      } catch (PutException e) {
        LOGGER.error("Could not delete item with key {} during handoff to {}.", key, peer, e);
      }
    }

    // Communicate sucess to ECS
    try {
      ecs.confirmHandoff();
    } catch (ClientException e) {
      LOGGER.error("Could not confirm handoff to ECS.", e);
    }
  }
}
