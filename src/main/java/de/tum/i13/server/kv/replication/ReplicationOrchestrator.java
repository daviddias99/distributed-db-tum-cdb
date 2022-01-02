package de.tum.i13.server.kv.replication;

import java.math.BigInteger;
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
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.RingRange;
import de.tum.i13.shared.hashing.RingRangeImpl;
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
      String upperBound = padLeftZeros(toDeleteRange.getEnd().toString(16), 16);
      try {
        List<String> keys = this.storage
            .getRange(lowerBound, upperBound)
            .stream()
            .map(e -> e.key)
            .collect(Collectors.toList());
        toDelete.addAll(keys);
      } catch (GetException e) {
        LOGGER.error("Could not fetch range for deletion.");
        e.printStackTrace();
      }
    }

    if (toDelete.isEmpty()) {
      return;
    }

    LOGGER.info("Deleting {} replicated keys from self.", toDelete.size());

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
    LOGGER.info("Deleting all replicated ranges");
    RingRange readRange = newRing.getReadRange(serverState.getCurNetworkLocation());
    RingRange writeRange = newRing.getWriteRange(serverState.getCurNetworkLocation());

    List<RingRange> replicationRanges = readRange.computeDifference(writeRange);
    this.deleteRanges(this.splitWrapping(replicationRanges));
  }

  private void deleteStaleRanges() {
    LOGGER.info("Deleting stale ranges");
    RingRange newReadRange = newRing.getReadRange(serverState.getCurNetworkLocation());
    RingRange oldReadRange = oldRing.getReadRange(serverState.getCurNetworkLocation());
    LOGGER.info("Stale ranges {} - {}", oldReadRange, newReadRange);

    List<RingRange> staleReplicationRanges = oldReadRange.computeDifference(newReadRange);
    this.deleteRanges(this.splitWrapping(staleReplicationRanges));
  }

  private void replicateToNewSuccessors(List<NetworkLocation> newSuccessors) {
    String lowerBound = padLeftZeros("0", 16);
    String upperBound = padLeftZeros("f", 16);
    try {
      // TODO: this won't work for real-life data situations (whole range can't
      // probably fit in memory)
      List<Pair<String>> toAdd = this.storage.getRange(lowerBound, upperBound);

      if (toAdd.isEmpty()) {
        return;
      }

      LOGGER.info("Sending {} keys to peers", toAdd.size());
      for (NetworkLocation peer : newSuccessors) {
        (new Thread(new BulkReplicationHandler(peer, toAdd))).start();
      }

    } catch (GetException e) {
      LOGGER.error("Could not fetch range for replication.");
    }
  }

  private List<Pair<String>> getRingRangeFromStorage(RingRange range) throws GetException {
    String lowerBound = padLeftZeros(range.getStart().toString(16), 16);
    String upperBound = padLeftZeros(range.getEnd().toString(16), 16);

    // TODO: this won't work for real-life data situations (whole range can't
    // probably fit in memory)
    return this.storage.getRange(lowerBound, upperBound);
  }

  private List<RingRange> splitRange(RingRange range) {
    List<RingRange> result = new LinkedList<>();
    HashingAlgorithm hashingAlgorithm = range.getHashingAlgorithm();

    // TODO instead of making RingRangeImpl public, it would be nice to have
    // something to split a range that wraps around
    RingRange range1 = new RingRangeImpl(range.getEnd(), hashingAlgorithm.getMax(), hashingAlgorithm);
    RingRange range2 = new RingRangeImpl(BigInteger.ZERO, range.getEnd(), hashingAlgorithm);
    result.add(range1);
    result.add(range2);
    return result;
  }

  private List<RingRange> splitWrapping(List<RingRange> toSplit) {
    List<RingRange> result = new LinkedList<>();

    for (RingRange range : toSplit) {
      if (range.wrapsAround()) {
        result.addAll(this.splitRange(range));
      } else {
        result.add(range);
      }
    }

    return result;
  }

  private void updateSuccessorReplication(NetworkLocation peer) {
    LOGGER.info("Updating {} replication.", peer);
    RingRange newWriteRange = newRing.getWriteRange(serverState.getCurNetworkLocation());
    RingRange oldWriteRange = oldRing.getWriteRange(serverState.getCurNetworkLocation());

    List<RingRange> newRangeToReplicate = this.splitWrapping(oldWriteRange.computeDifference(newWriteRange));

    for (RingRange ringRange : newRangeToReplicate) {
      try {

        List<Pair<String>> toAdd = this.getRingRangeFromStorage(ringRange);

        if (toAdd.isEmpty()) {
          return;
        }

        LOGGER.info("Sending {} keys to peers", toAdd.size());

        (new Thread(new BulkReplicationHandler(peer, toAdd))).start();
      } catch (GetException e) {
        LOGGER.error("Could not fetch range for replication.");
      }
    }
  }

  public synchronized void handleKeyRangeChange(ConsistentHashRing oldRing, ConsistentHashRing newRing) {
    this.oldRing = oldRing;
    this.newRing = newRing;

    if (oldRing == null || !newRing.isReplicationActive()) {
      return;
    }

    // If no more replication, delete all chunks in read range
    if (oldRing.isReplicationActive() && !newRing.isReplicationActive()) {
      this.deleteReplicatedRanges();
      return;
    }

    // Check if successors changed
    // If new one, send new one all chunks
    int oldReplicatorCount = Math.min(Constants.NUMBER_OF_REPLICAS, oldRing.size() - 1);
    int newReplicatorCount = Math.min(Constants.NUMBER_OF_REPLICAS, newRing.size() - 1);
    NetworkLocation currentLocation = this.serverState.getCurNetworkLocation();
    List<NetworkLocation> oldReplicators = oldRing.getSucceedingNetworkLocations(currentLocation, oldReplicatorCount);
    List<NetworkLocation> newReplicators = newRing.getSucceedingNetworkLocations(currentLocation, newReplicatorCount);
    List<NetworkLocation> newSuccessors = newReplicators
        .stream()
        .filter(replicator -> !oldReplicators.contains(replicator))
        .collect(Collectors.toList());

    this.replicateToNewSuccessors(newSuccessors);

    // Get write range changes
    // If succ(2) still the same
    // Send Inc(range) to succ(2)

    if (oldReplicatorCount > 0
        && oldReplicators.get(oldReplicatorCount - 1).equals(newReplicators.get(newReplicatorCount - 1))) {
      this.updateSuccessorReplication(newReplicators.get(newReplicatorCount - 1));
    }

    // Get read range changes
    // Delete Dec(range) from self
    this.deleteStaleRanges();
  }
}
