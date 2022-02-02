package de.tum.i13.server.kvchord;

import de.tum.i13.server.kv.commandprocessing.handlers.AsyncDeleteHandler;
import de.tum.i13.server.kv.commandprocessing.handlers.BulkReplicationHandler;
import de.tum.i13.server.kvchord.commandprocessing.handlers.HandoffHandler;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;

public class KVChordListener implements ChordListener {

    private static final Logger LOGGER = LogManager.getLogger(KVChordListener.class);
    private static final ScheduledExecutorService DELETION_EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor();
    private static final Set<ScheduledFuture<?>> DELETION_TASKS = new HashSet<>();
    public static final int REPLICATION_DELETION_DELAY = 5000;

    private ChordServerState state;
    private boolean doAsyncHandoff = false;
    private PersistentStorage storage;
    private HashingAlgorithm hashing;
    private NetworkLocation predecessorChangeMemory;
    private NetworkLocation lastPredecessor;
    private int maxRetryCount = 3;
    private int retryWaitSec = 2;

    public KVChordListener(ChordServerState state, PersistentStorage storage, HashingAlgorithm hashing) {
        this.state = state;
        this.storage = storage;
        this.hashing = hashing;
        this.predecessorChangeMemory = NetworkLocation.NULL;
        this.lastPredecessor = NetworkLocation.NULL;
    }

    @Override
    public synchronized void predecessorChanged(NetworkLocation previous, NetworkLocation current) {
        LOGGER.info("Predecessor changed from {} to {}", previous, current);
        this.lastPredecessor = current;
        if (current.equals(previous)) {
            return;
        }

        if (current.equals(NetworkLocation.NULL)) {
            this.predecessorChangeMemory = previous;
            return;
        }

        if (state.getCurNetworkLocation().equals(current) || previous.equals(NetworkLocation.NULL)) {
            return;
        }

        this.state.writeLock();

        String lowerBound = this.hashing
                .hash(previous.equals(NetworkLocation.NULL) ? this.predecessorChangeMemory : previous)
                .toString(16);
        String upperBound = this.hashing.hash(current).toString(16);
        LOGGER.info("Trying to execute handoff (async={}) of [{}-{}]", doAsyncHandoff, lowerBound, upperBound);

        Runnable handoff = new HandoffHandler(current, lowerBound, upperBound, this.storage, this.state, this.hashing);

        if (doAsyncHandoff) {
            Thread handoffProcess = new Thread(handoff);
            handoffProcess.start();
            LOGGER.info("Started async handoff process");
        } else {
            handoff.run();
            LOGGER.info("Finished sync. handoff.");
        }

        // Remove write lock
        this.state.start();
    }

    @Override
    public void successorChanged(NetworkLocation previous, NetworkLocation current) {
        // TODO Auto-generated method stub

    }

    private List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {

        int hashSize = this.hashing.getHashSizeBits() / 4;
        String paddedLower = HashingAlgorithm.padLeftZeros(lowerBound, hashSize);
        String paddedUpper = HashingAlgorithm.padLeftZeros(upperBound, hashSize);

        if (paddedLower.compareTo(paddedUpper) <= 0) {
            return this.storage.getRange(paddedLower, paddedUpper);
        }

        List<Pair<String>> result = new LinkedList<>();
        result.addAll(this.storage.getRange("0".repeat(hashSize), paddedUpper));
        result.addAll(this.storage.getRange(paddedLower, "f".repeat(hashSize)));

        return result;
    }

    private List<Pair<String>> getRelevantRangeFromStorage() throws GetException {

        NetworkLocation bestEffortPredecesor = NetworkLocation.NULL;
        for (int i = 0; i < this.maxRetryCount; i++) {
            bestEffortPredecesor = NetworkLocation.NULL.equals(this.lastPredecessor)
                    ? this.predecessorChangeMemory
                    : this.lastPredecessor;

            if (!NetworkLocation.NULL.equals(bestEffortPredecesor)) {
                break;
            }
            try {
                Thread.sleep(retryWaitSec * 1000l);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        if (NetworkLocation.NULL.equals(bestEffortPredecesor)) {
            return new LinkedList<>();
        }

        NetworkLocation currentLocation = this.state.getCurNetworkLocation();

        // TODO: this won't work for real-life data situations (whole range can't
        // probably fit in memory)
        LOGGER.info("{} and {}", currentLocation, bestEffortPredecesor);
        return this.getRange(this.hashing.hash(bestEffortPredecesor).toString(16),
                this.hashing.hash(currentLocation).toString(16));
    }

    public void deleteReplicatedRanges(boolean async) {
        LOGGER.info("Deleting replicated ranges");
        NetworkLocation bestEffortPredecesor = NetworkLocation.NULL.equals(this.lastPredecessor)
                ? this.predecessorChangeMemory
                : this.lastPredecessor;

        NetworkLocation currentLocation = this.state.getCurNetworkLocation();
        BigInteger currentLocationHashValue = this.hashing.hash(currentLocation);
        String lowerBound = currentLocationHashValue.equals(this.hashing.getMax()) ? BigInteger.ZERO.toString(16)
                : this.hashing.hash(currentLocation).add(BigInteger.ONE).toString(16);

        try {
            List<String> toDelete = this
                    .getRange(lowerBound, this.hashing.hash(bestEffortPredecesor).toString(16))
                    .stream()
                    .map(e -> e.key)
                    .collect(Collectors.toList());
            if (async) {
                (new Thread(new AsyncDeleteHandler(storage, toDelete))).start();
            } else {
                (new AsyncDeleteHandler(storage, toDelete)).run();
            }
        } catch (GetException e) {
            LOGGER.atError().withThrowable(e).log("Could not fetch range for replicated range deletion");
        }
    }

    @Override
    public void successorsChanged(List<NetworkLocation> previous, List<NetworkLocation> current) {
        final boolean wasReplicated = previous.size() >= Constants.NUMBER_OF_REPLICAS;
        final boolean isReplicated = current.size() >= Constants.NUMBER_OF_REPLICAS;
        final boolean replicationChangedOrOff = !(wasReplicated && isReplicated);

        if (replicationChangedOrOff) {
            if (wasReplicated) scheduleReplicationDeletion();
            else if (isReplicated) replicateFullRange(current);
            return;
        }

        Set<NetworkLocation> removals = (new HashSet<>(previous.subList(0, Constants.NUMBER_OF_REPLICAS)));
        removals.removeAll(current);
        Set<NetworkLocation> enters = (new HashSet<>(current.subList(0, Constants.NUMBER_OF_REPLICAS)));
        enters.removeAll(previous);
        if (removals.isEmpty() && enters.isEmpty()) return;

        var optRelevantElements = getRelevantElementsSafely();
        if (optRelevantElements.isEmpty()) return;

        var relevantElements = optRelevantElements.get();
        sendChangesToSuccessors(removals, enters, relevantElements);
    }

    private void sendChangesToSuccessors(Set<NetworkLocation> removals, Set<NetworkLocation> enters,
                                         List<Pair<String>> relevantElements) {
        // If new successors, send them range
        for (NetworkLocation newSucc : enters) {
            LOGGER.info("Sending {} keys to peers {}", relevantElements.size(), newSucc);
            (new Thread(new BulkReplicationHandler(newSucc, relevantElements))).start();
        }

        // If old successors, send them delete notice (if possible)
        // TODO: can be much more efficient using a DELETE_RANGE
        for (NetworkLocation oldSucc : removals) {
            LOGGER.info("Deleting {} keys from peer {}", relevantElements.size(), oldSucc);
            (new Thread(new BulkReplicationHandler(oldSucc, relevantElements, true))).start();
        }
    }

    private Optional<List<Pair<String>>> getRelevantElementsSafely() {
        try {
            return Optional.of(this.getRelevantRangeFromStorage())
                    .filter(not(List::isEmpty));
        } catch (GetException e) {
            LOGGER.atError().withThrowable(e).log("Could not get range to replicate from storage");
            return Optional.empty();
        }
    }

    private void replicateFullRange(List<NetworkLocation> current) {
        // TODO It might still be necessary to let the tasks to execute, if for example the previous
        //  predecessor is different from the new predecessor
        LOGGER.info("Canceling potential previous deletion tasks");
        for (Iterator<ScheduledFuture<?>> iterator = DELETION_TASKS.iterator(); iterator.hasNext(); ) {
            iterator.next().cancel(true);
            iterator.remove();
        }

        LOGGER.info("Replicating full range");
        List<Pair<String>> relevantElements;
        try {
            relevantElements = this.getRelevantRangeFromStorage();
        } catch (GetException e) {
            LOGGER.atError().withThrowable(e).log("Could not get range to replicate from storage");
            return;
        }

        // If new successors, send them range
        for (NetworkLocation newSucc : current) {
            LOGGER.info("Sending {} keys to peers {}", relevantElements.size(), newSucc);
            (new Thread(new BulkReplicationHandler(newSucc, relevantElements))).start();
        }
    }

    private void scheduleReplicationDeletion() {
        final var deletionTask = DELETION_EXECUTOR_SERVICE.schedule(
                () -> this.deleteReplicatedRanges(true), REPLICATION_DELETION_DELAY, MILLISECONDS);
        DELETION_TASKS.add(deletionTask);
    }

}
