package de.tum.i13.server.kv.commandprocessing;

import java.util.LinkedList;
import java.util.List;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.NetworkMessageServer;
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

  private PersistentStorage storage;
  private NetworkLocation ecs;
  private NetworkLocation peer;
  private String lowerBound;
  private String upperBound;

  public HandoffHandler(NetworkLocation peer, NetworkLocation ecs, String lowerBound, String upperBound, PersistentStorage storage) {
    this.storage = storage;
    this.peer = peer;
    this.ecs = ecs;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public void run() {
    NetworkMessageServer messageServer = new CommunicationClient();
    ServerCommunicator communicator = new ServerCommunicator(messageServer);
    NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(messageServer);
    List<Pair<String>> items;
    
    List<String> nodesToDelete = new LinkedList<>();

    try {
      items = this.storage.getRange(lowerBound, upperBound);
      netPeerStorage.connect(peer.getAddress(), peer.getPort());

      // Send items to peer
      for (Pair<String> item : items) {
        KVMessage response = netPeerStorage.put(item.key, item.value);

        if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
          nodesToDelete.add(item.key);
        }
      }

      // Delete items after sending (only sucessful ones)
      for (String key : nodesToDelete) {
        storage.put(key, null);
      }

      // Communicate sucess to ECS
      communicator.connect(ecs.getAddress(), ecs.getPort());
      communicator.confirmHandoff();
    } catch (ClientException e) {
      // TODO Auto-generated catch block
    } catch (PutException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (GetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
