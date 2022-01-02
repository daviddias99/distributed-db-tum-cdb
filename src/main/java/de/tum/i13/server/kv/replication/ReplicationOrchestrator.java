package de.tum.i13.server.kv.replication;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.commandprocessing.handlers.BulkReplicationHandler;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.RingRange;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

public class ReplicationOrchestrator {

  private static final Logger LOGGER = LogManager.getLogger(ReplicationOrchestrator.class);

  private final ServerState serverState;
  private final PersistentStorage storage;
  private ConsistentHashRing oldRing;
  private ConsistentHashRing newRing;

  public ReplicationOrchestrator(ServerState serverState, PersistentStorage storage) {
    this.serverState = serverState;
    this.storage = storage;
  }

  // TODO: Think of a way to avoid having to pad strings for get range. The
  // problem is that the BTree implementation respects the lexicographic order of
  // the keys. However, the conversions from BigInteger to hex string don't pad
  // the values with zeros.
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

  private void deleteRanges(List<RingRange> toDeleteRanges) {
    List<String> toDelete = new LinkedList<>();

    for (RingRange toDeleteRange : toDeleteRanges) {

      String lowerBound = padLeftZeros(toDeleteRange.getStart().toString(16), 16);
      String upperBound = padLeftZeros(toDeleteRange.getStart().toString(16), 16);
      try {
        toDelete.addAll(
            this.storage.getRange(lowerBound, upperBound).stream().map(e -> e.key).collect(Collectors.toList()));
      } catch (GetException e) {
        LOGGER.error("Could not fetch range for deletion.");
        e.printStackTrace();
      }
    }

    (new Thread() {
      @Override
      public void run() {
        for (String key : toDelete) {
          try {
            storage.put(key, null);
          } catch (PutException e) {
            LOGGER.error("Could not delete item with key {}.", key);
          }
        }
      }
    }).start();
  }

  private void deleteReplicatedRanges() {
    LOGGER.info("Deleting all replicated ranges") ;
    RingRange readRange = newRing.getReadRange(serverState.getCurNetworkLocation());
    RingRange writeRange = newRing.getWriteRange(serverState.getCurNetworkLocation());

    List<RingRange> replicationRanges = readRange.computeDifference(writeRange);
    this.deleteRanges(replicationRanges);
  }
  

  private void deleteStaleRanges() {
    LOGGER.info("Deleting stale ranges") ;
    RingRange newReadRange = newRing.getReadRange(serverState.getCurNetworkLocation());
    RingRange oldReadRange = oldRing.getReadRange(serverState.getCurNetworkLocation());

    List<RingRange> staleReplicationRanges = oldReadRange.computeDifference(newReadRange);
    this.deleteRanges(staleReplicationRanges);
  }

  private void replicateToNewSuccessors(List<NetworkLocation> newSuccessors) {
    String lowerBound = padLeftZeros("0", 16);
    String upperBound = padLeftZeros("f", 16);
    try {
      // TODO: this won't work for real-life data situations (whole range can't probably fit in memory)
      List<Pair<String>> toAdd = this.storage.getRange(lowerBound, upperBound);
      LOGGER.info("Sending {} keys to peers", toAdd.size()) ;
      for (NetworkLocation peer : newSuccessors) {
        (new Thread(new BulkReplicationHandler(peer, toAdd))).start();
      }

    } catch (GetException e) {
      LOGGER.error("Could not fetch range for replication.");
    }
  }

  private void updateSuccessorReplication(NetworkLocation peer) {
    LOGGER.info("Updating {} replication.", peer) ;
    RingRange newWriteRange = newRing.getWriteRange(serverState.getCurNetworkLocation());
    RingRange oldWriteRange = oldRing.getWriteRange(serverState.getCurNetworkLocation());

    List<RingRange> newRangeToReplicate = oldWriteRange.computeDifference(newWriteRange);

    for (RingRange ringRange : newRangeToReplicate) {
      String lowerBound = padLeftZeros(ringRange.getStart().toString(16), 16);
      String upperBound = padLeftZeros(ringRange.getEnd().toString(16), 16);
  
      try {
        // TODO: this won't work for real-life data situations (whole range can't probably fit in memory)
        List<Pair<String>> toAdd = this.storage.getRange(lowerBound, upperBound);
        LOGGER.info("Sending {} keys to peers", toAdd.size()) ;

        (new Thread(new BulkReplicationHandler(peer, toAdd))).start();
      } catch (GetException e) {
        LOGGER.error("Could not fetch range for replication.");
      }
    }
  }

  public synchronized void handleKeyRangeChange(ConsistentHashRing oldRing, ConsistentHashRing newRing) {
    this.oldRing = oldRing;
    this.newRing = newRing;

    // If no more replication, delete all chunks in read range
    if (!newRing.isReplicationActive()) {
      this.deleteReplicatedRanges();
      return;
    }

    // Check if successors changed
    // If new one, send new one all chunks
    List<NetworkLocation> oldReplicators = oldRing.getSucceedingNetworkLocations(this.serverState.getCurNetworkLocation(), Constants.NUMBER_OF_REPLICAS);
    List<NetworkLocation> newReplicators = oldRing.getSucceedingNetworkLocations(this.serverState.getCurNetworkLocation(), Constants.NUMBER_OF_REPLICAS);
    int newReplicaCount = newReplicators.size();
    List<NetworkLocation> newSuccessors = newReplicators.stream()
        .filter(replicator -> !oldReplicators.contains(replicator))
        .collect(Collectors.toList());

    this.replicateToNewSuccessors(newSuccessors);
      
    // Get write range changes
    // If succ(2) still the same
    // Send Inc(range) to succ(2)

    if (oldReplicators.get(newReplicaCount-1).equals(newReplicators.get(newReplicaCount-1))) {
      this.updateSuccessorReplication(newReplicators.get(newReplicaCount-1));
    }

    // Get read range changes
    // Delete Dec(range) from self
    this.deleteStaleRanges();
  }
}
