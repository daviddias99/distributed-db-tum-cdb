package de.tum.i13.server.kv.commandprocessing;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.client.net.NetworkMessageServer;
import de.tum.i13.client.net.NetworkPersistentStorage;
import de.tum.i13.client.net.WrappingPersistentStorage;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.GetException;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.persistentstorage.PutException;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.hashing.ConsistentHashRing;

public class KVEcsCommandProcessor implements CommandProcessor<KVMessage> {

  private static final Logger LOGGER = LogManager.getLogger(KVEcsCommandProcessor.class);

  private ServerState serverState;
  private PersistentStorage storage;

  public KVEcsCommandProcessor(PersistentStorage storage, ServerState serverState) {
    this.serverState = serverState;
    this.storage = storage;
  }

  @Override
  public KVMessage process(KVMessage command, PeerType peerType) {

    if (peerType != PeerType.ECS) {
      return null;
    }

    return switch (command.getStatus()) {
      case HEART_BEAT -> new KVMessageImpl(KVMessage.StatusType.HEART_BEAT);
      case ECS_WRITE_LOCK -> this.writeLock();
      case ECS_WRITE_UNLOCK -> this.writeUnlock();
      case ECS_HANDOFF -> this.handoff(command);
      case ECS_SET_KEYRANGE -> this.setKeyRange(command);
      default -> null;
    };
  }

  private KVMessage writeLock() {
    this.serverState.writeLock();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
  }

  private KVMessage writeUnlock() {
    this.serverState.start();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  public KVMessage setKeyRange(KVMessage command) {
    this.serverState.setRingMetadata(ConsistentHashRing.unpackMetadata(command.getKey()));
    this.serverState.start();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  private KVMessage handoff(KVMessage command) {
    String[] bounds = command.getValue().split(" ");

    try {
      List<Pair<String>> items = this.storage.getRange(bounds[0], bounds[1]);

      new Runnable(){
        public void run(){
          NetworkMessageServer messageServer = new CommunicationClient();
          ServerCommunicator communicator = new ServerCommunicator(messageServer);
          NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(messageServer);
          NetworkLocation peerLocation = NetworkLocation.extractNetworkLocation(command.getKey());
          List<String> nodesToDelete = new LinkedList<>();

          try {
            netPeerStorage.connect(peerLocation.getAddress(), peerLocation.getPort());

            for (Pair<String> item : items) {
              KVMessage response = netPeerStorage.put(item.key, item.value);

              if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                nodesToDelete.add(item.key);
              }
            }

            for (String key : nodesToDelete) {
              storage.put(key, null);
            }     

            communicator.connect(serverState.getEcsLocation().getAddress(), serverState.getEcsLocation().getPort());
            communicator.confirmHandoff();

          } catch (ClientException e) {
            // TODO Auto-generated catch block
          } catch (PutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }
      };  

      return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
    } catch (GetException e) {
      return new KVMessageImpl(KVMessage.StatusType.ERROR);
    }

  }
}
