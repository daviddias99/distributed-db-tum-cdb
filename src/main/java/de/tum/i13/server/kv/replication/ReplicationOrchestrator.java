package de.tum.i13.server.kv.replication;

import de.tum.i13.server.kv.commandprocessing.handlers.AsyncDeleteHandler;
import de.tum.i13.server.kv.commandprocessing.handlers.BulkReplicationHandler;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.RingRange;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static de.tum.i13.shared.SharedUtils.withExceptionsLogged;

public class ReplicationOrchestrator {

    private static final Logger LOGGER = LogManager.getLogger(ReplicationOrchestrator.class);

    private final NetworkLocation curNetworkLocation;
    private final PersistentStorage storage;
    private ConsistentHashRing oldRing;
    private ConsistentHashRing newRing;

    public ReplicationOrchestrator(NetworkLocation curNetworkLocation, PersistentStorage storage) {
        this.curNetworkLocation = curNetworkLocation;
        this.storage = storage;
    }

    private void deleteRanges(List<RingRange> toDeleteRanges) {
        List<String> toDelete = new LinkedList<>();

        for (RingRange toDeleteRange : toDeleteRanges) {
            try {
                List<String> keys = this
                        .getRingRangeFromStorage(toDeleteRange)
                        .stream()
                        .map(e -> e.key)
                        .collect(Collectors.toList());
                toDelete.addAll(new LinkedList<>(keys));
            } catch (GetException e) {
                LOGGER.error("Could not fetch range for deletion.", e);
            }
        }

        if (toDelete.isEmpty()) {
            return;
        }
        (new Thread(withExceptionsLogged(new AsyncDeleteHandler(storage, toDelete)))).start();
    }

    private void deleteReplicatedRanges() {
        LOGGER.info("Deleting all replicated ranges");
        RingRange readRange = oldRing.getReadRange(curNetworkLocation);
        RingRange writeRange = newRing.getWriteRange(curNetworkLocation);
        LOGGER.info("Replicated ranges {} - {}", readRange, writeRange);

        List<RingRange> replicationRanges = readRange.computeDifference(writeRange);
        this.deleteRanges(this.splitWrapping(replicationRanges));
    }

    public void deleteReplicatedRangesWithoutUpdate() {
        LOGGER.info("Deleting all replicated ranges");
        RingRange readRange = newRing.getReadRange(curNetworkLocation);
        RingRange writeRange = newRing.getWriteRange(curNetworkLocation);
        LOGGER.info("Replicated ranges {} - {}", readRange, writeRange);

        List<RingRange> replicationRanges = readRange.computeDifference(writeRange);
        this.deleteRanges(this.splitWrapping(replicationRanges));
    }

    private void deleteStaleRanges() {
        LOGGER.info("Deleting stale ranges");
        RingRange newReadRange = newRing.getReadRange(curNetworkLocation);
        RingRange oldReadRange = oldRing.getReadRange(curNetworkLocation);
        LOGGER.info("Stale ranges {} - {}", oldReadRange, newReadRange);

        List<RingRange> staleReplicationRanges = oldReadRange.computeDifference(newReadRange);
        this.deleteRanges(this.splitWrapping(staleReplicationRanges));
    }

    private void replicateToNewSuccessors(List<NetworkLocation> newSuccessors, RingRange range) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Replicating range %s to new successors:%n", range));

        for (NetworkLocation networkLocation : newSuccessors) {
            sb.append(String.format("- %s%n", networkLocation));
        }

        LOGGER.info(sb);
        List<Pair<String>> toAdd = new LinkedList<>();

        for (RingRange splitRange : range.getAsNonWrapping()) {
            try {
                toAdd.addAll(this.getRingRangeFromStorage(splitRange));
            } catch (GetException e) {
                LOGGER.error("Could not fetch range for replication.", e);
            }
        }

        if (toAdd.isEmpty())
            return;

        LOGGER.info("Sending {} keys to peers", toAdd.size());
        for (NetworkLocation peer : newSuccessors) {
            (new Thread(withExceptionsLogged(new BulkReplicationHandler(peer, toAdd)))).start();
        }
    }

    private List<Pair<String>> getRingRangeFromStorage(RingRange range) throws GetException {
        final int hashLength = range.getHashingAlgorithm().getHashSizeBits() / Constants.BITS_PER_HEX_CHARACTER;
        String lowerBound = HashingAlgorithm.padLeftZeros(range.getStart().toString(16), hashLength);
        String upperBound = HashingAlgorithm.padLeftZeros(range.getEnd().toString(16), hashLength);

        // TODO: this won't work for real-life data situations (whole range can't
        // probably fit in memory)
        LOGGER.trace("Getting range {} from storage", range);
        return this.storage.getRange(lowerBound, upperBound);
    }

    private List<RingRange> splitWrapping(List<RingRange> toSplit) {
        List<RingRange> result = new LinkedList<>();

        for (RingRange range : toSplit) {
            result.addAll(range.getAsNonWrapping());
        }

        return result;
    }

    private void updateSuccessorReplication(NetworkLocation peer) {
        LOGGER.info("Updating {} replication.", peer);
        RingRange newWriteRange = newRing.getWriteRange(curNetworkLocation);
        RingRange oldWriteRange = oldRing.getWriteRange(curNetworkLocation);

        List<RingRange> newRangeToReplicate = this.splitWrapping(oldWriteRange.computeDifference(newWriteRange));

        for (RingRange ringRange : newRangeToReplicate) {
            try {

                List<Pair<String>> toAdd = this.getRingRangeFromStorage(ringRange);

                if (toAdd.isEmpty())
                    return;

                LOGGER.info("Sending {} keys ({}) to {}", toAdd.size(), ringRange, peer);
                (new Thread(withExceptionsLogged(new BulkReplicationHandler(peer, toAdd)))).start();
            } catch (GetException e) {
                LOGGER.error("Could not fetch range for replication.", e);
            }
        }
    }

    public void handleKeyRangeChange(ConsistentHashRing oldRing, ConsistentHashRing newRing) {
        this.oldRing = oldRing;
        this.newRing = newRing;

        if (oldRing == null) {
            return;
        }

        // If no more replication, delete all chunks in read range
        if (!newRing.isReplicationActive()) {
            if (oldRing.isReplicationActive())
                this.deleteReplicatedRanges();
            return;
        }

        int oldReplicatorCount = Math.min(Constants.NUMBER_OF_REPLICAS, oldRing.size() - 1);
        int newReplicatorCount = Math.min(Constants.NUMBER_OF_REPLICAS, newRing.size() - 1);
        List<NetworkLocation> oldReplicators = oldRing.getSucceedingNetworkLocations(curNetworkLocation,
                oldReplicatorCount);
        List<NetworkLocation> newReplicators = newRing.getSucceedingNetworkLocations(curNetworkLocation,
                newReplicatorCount);

        // Replicate whole range to new successors (if any)
        List<NetworkLocation> newSuccessors = oldRing.isReplicationActive() ? newReplicators
                .stream()
                .filter(replicator -> !oldReplicators.contains(replicator))
                .collect(Collectors.toList()) : newReplicators;

        this.replicateToNewSuccessors(new LinkedList<>(newSuccessors), newRing.getWriteRange(curNetworkLocation));

        // If old replicator, update only with new range
        if (oldReplicatorCount > 0 && oldRing.isReplicationActive()) {
            for (int i = Constants.NUMBER_OF_REPLICAS - 1; i < newReplicators.size(); i++) {
                if (oldReplicators.contains(newReplicators.get(i))) {
                    this.updateSuccessorReplication(newReplicators.get(i));
                }
            }
        }

        // Delete ranges that are no longer replicated
        this.deleteStaleRanges();
    }

}
