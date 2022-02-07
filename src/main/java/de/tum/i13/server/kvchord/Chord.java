package de.tum.i13.server.kvchord;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import io.vavr.control.Try;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.tum.i13.shared.SharedUtils.withExceptionsLogged;

public class Chord {

    private static final Logger LOGGER = LogManager.getLogger(Chord.class);

    public static final int STABILIZATION_INTERVAL = 1000;
    public static final int FIX_FINGERS_INTERVAL = 500;
    public static final int CHECK_PREDECESSORS_INTERVAL = 1000;
    public static final int STABILIZATION_START_OFFSET = 0;
    public static final int FIX_FINGERS_START_OFFSET = 200;
    public static final int CHECK_PREDECESSORS_START_OFFSET = 400;

    private final int TABLE_SIZE;
    private final int SUCCESSOR_LIST_SIZE = Constants.NUMBER_OF_REPLICAS;

    private final HashingAlgorithm hashingAlgorithm;
    private final ChordMessaging messaging;

    private int fingerTableUpdateIndex;
    private final NetworkLocation ownLocation;
    private final NetworkLocation bootstrapNode;
    private NetworkLocation predecessor;
    private ChordSuccessorList successors; // Maybe this and the finger table can be merged
    private final ConcurrentNavigableMap<BigInteger, NetworkLocation> fingerTable;
    private final List<BigInteger> fingerTableKeys;

    private final List<ChordListener> listeners;

    /* CHORD */

    public Chord(HashingAlgorithm hashingAlgorithm, NetworkLocation ownLocation) {
        this(hashingAlgorithm, ownLocation, null);
    }

    public Chord(HashingAlgorithm hashingAlgorithm, NetworkLocation ownLocation, NetworkLocation bootstrapNode) {
        this.hashingAlgorithm = hashingAlgorithm;
        this.ownLocation = ownLocation;
        this.predecessor = ownLocation; // TODO: check this
        this.TABLE_SIZE = hashingAlgorithm.getHashSizeBits();
        this.fingerTable = new ConcurrentSkipListMap<>();
        this.fingerTableKeys = new ArrayList<>();
        this.listeners = new LinkedList<>();
        this.successors = new ChordSuccessorList(SUCCESSOR_LIST_SIZE, ownLocation, fingerTable, hashingAlgorithm,
                this.listeners);
        this.messaging = new ChordMessaging(this);
        this.bootstrapNode = bootstrapNode;
        this.initFingerTable(ownLocation);
    }

    private void doBoostrap() throws ChordException {
        NetworkLocation successor = this.messaging.findSuccessor(bootstrapNode,
                this.hashingAlgorithm.hash(ownLocation));

        if (successor.equals(NetworkLocation.NULL)) {
            LOGGER.error("Could not boostrap chord instance with {}", bootstrapNode);
            throw new ChordException(
                    String.format("An error occured while boostrapping Chord with %s", bootstrapNode));
        }

        this.successors.setFirst(successor);
        messaging.notifyNode(successor);
    }

    public void start() throws ChordException {
        if (bootstrapNode != null) {
            this.doBoostrap();
        }
        this.initThreads();
    }

    private NetworkLocation[] findPredecessor(BigInteger key) throws ChordException {
        NetworkLocation nPrime = this.ownLocation;
        NetworkLocation nPrimeSuccessor = this.getSuccessor();

        if (nPrimeSuccessor.equals(NetworkLocation.NULL)) {
            return new NetworkLocation[]{nPrime, nPrime};
        }

        while (!this.betweenTwoKeys(
                hashingAlgorithm.hash(nPrime),
                hashingAlgorithm.hash(nPrimeSuccessor),
                key,
                false,
                true)) {
            NetworkLocation newNPrime = this.messaging.closestPrecedingFinger(nPrime, key);

            if (newNPrime.equals(NetworkLocation.NULL)) {
                LOGGER.error("Could not get closest preceding finger from {}", nPrime);
                throw new ChordException("Could not get closest preceding finger");
            }

            // Avoid infinite loop
            if (newNPrime == nPrime) {
                break;
            }

            nPrime = newNPrime;
            nPrimeSuccessor = this.messaging.getSuccessor(nPrime);

            if (nPrimeSuccessor.equals(NetworkLocation.NULL)) {
                LOGGER.error("Could not get successor from {}", nPrime);
                throw new ChordException("Could not get sucessor");
            }
        }

        return new NetworkLocation[]{nPrime, nPrimeSuccessor};
    }

    public NetworkLocation findSuccessor(BigInteger key) throws ChordException {

        if (key.equals(this.hashingAlgorithm.hash(this.ownLocation))) {
            return this.ownLocation;
        }

        return this.findPredecessor(key)[1];
    }

    public NetworkLocation closestPrecedingFinger(BigInteger key) {
        for (BigInteger mapKey : this.fingerTable.descendingKeySet()) {
            NetworkLocation value = this.fingerTable.get(mapKey);

            if (this.betweenTwoKeys(
                    this.hashingAlgorithm.hash(this.ownLocation),
                    key,
                    this.hashingAlgorithm.hash(value),
                    false,
                    false)) {
                return value;
            }
        }

        return this.ownLocation;
    }

    public void notifyNode(NetworkLocation peer) {

        boolean isBetweenKeys = this.betweenTwoKeys(
                hashingAlgorithm.hash(predecessor),
                hashingAlgorithm.hash(ownLocation),
                hashingAlgorithm.hash(peer),
                false,
                false);

        if (this.predecessor.equals(NetworkLocation.NULL) || isBetweenKeys) {
            this.setPredecessor(peer);
        }
    }

    private StabilizationData querySuccessorForStabilization() {
        while (true) {
            NetworkLocation successor = this.getSuccessor();

            if (successor.equals(NetworkLocation.NULL)) {
                return null;
            }

            try {
                NetworkLocation succPredecessor = messaging.getPredecessor(successor);
                List<NetworkLocation> succSuccessors = messaging.getSuccessors(successor, this.SUCCESSOR_LIST_SIZE);
                return new StabilizationData(succPredecessor, succSuccessors);
            } catch (ChordException e) {
                // Could not reach successor, change to next successor in list
                this.successors.shift();
                LOGGER.error("Failed to communicate with successor");
            }
            if (successor.equals(this.getSuccessor())) {
                break;
            }
        }

        return null;
    }

    private void stabilize() {
        NetworkLocation successor = this.getSuccessor();

        // Check if successor is self
        if (successor.equals(this.ownLocation) || successor.equals(NetworkLocation.NULL)) {
            if (!this.getPredecessor().equals(NetworkLocation.NULL) && !this.getPredecessor().equals(this.ownLocation)) {
                this.successors.setFirst(this.getPredecessor());
            }
            return;
        }

        StabilizationData stabilizationData = this.querySuccessorForStabilization();

        if (stabilizationData == null) {
            LOGGER.error("Could not stabilize");
            return;
        }

        NetworkLocation successorPredecessor = stabilizationData.successorPredecessor;
        List<NetworkLocation> successorSuccessors = stabilizationData.successorSuccessors;
        this.successors.update(successorSuccessors);

        // Check if successor predecessor is self
        if (this.ownLocation.equals(successorPredecessor)) {
            // No need to notify
            return;
        }

        // Re-fetch successor because it might have changed during the stabilization
        // query
        successor = this.getSuccessor();

        if (successorPredecessor.equals(NetworkLocation.NULL)) {
            messaging.notifyNode(successor);
            return;
        }

        boolean setNewSuccessor = this.betweenTwoKeys(
                hashingAlgorithm.hash(ownLocation),
                hashingAlgorithm.hash(successor),
                hashingAlgorithm.hash(successorPredecessor),
                false,
                false);

        if (setNewSuccessor) {
            this.successors.setFirst(successorPredecessor);
        }

        messaging.notifyNode(this.getSuccessor());
    }

    private void fixFingers() {
        Map.Entry<BigInteger, NetworkLocation> toUpdate = this.advanceUpdateIteratorIterator();
        try {
            NetworkLocation newFinger = this.findSuccessor(toUpdate.getKey());

            if (newFinger.equals(NetworkLocation.NULL)) {
                return;
            }

            this.fingerTable.put(toUpdate.getKey(), newFinger);
            LOGGER.debug("Fixed finger {} to {}", toUpdate.getKey().toString(16), newFinger);
        } catch (ChordException e) {
            LOGGER.error("Could not fix finger {}", toUpdate.getKey().toString(16));
        }
    }

    private void checkPredecessor() {
        if (this.ownLocation.equals(this.predecessor) || this.predecessor.equals(NetworkLocation.NULL)) {
            return;
        }
        LOGGER.info("Sending (self: {}) heartbeat to {}", this.ownLocation, this.predecessor);
        boolean predecessorAlive = this.messaging.isNodeAlive(this.predecessor);

        if (!predecessorAlive) {
            this.setPredecessor(NetworkLocation.NULL);
        }
    }

    private void initThreads() {
        final ScheduledExecutorService periodicThreadPool = Executors.newScheduledThreadPool(3);

        // The threads are started with a delay to avoid them running at the same time
        periodicThreadPool.scheduleWithFixedDelay(withExceptionsLogged(this::stabilize),
                STABILIZATION_START_OFFSET, STABILIZATION_INTERVAL, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleWithFixedDelay(withExceptionsLogged(this::fixFingers),
                FIX_FINGERS_START_OFFSET, FIX_FINGERS_INTERVAL, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleWithFixedDelay(withExceptionsLogged(this::checkPredecessor),
                CHECK_PREDECESSORS_START_OFFSET, CHECK_PREDECESSORS_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void setPredecessor(NetworkLocation predecessor) {
        NetworkLocation oldPredecessor = this.predecessor;
        this.predecessor = predecessor;

        this.listeners.forEach(list -> list.predecessorChanged(oldPredecessor, predecessor));
    }

    /* HELPER */

    private static class StabilizationData {

        public final NetworkLocation successorPredecessor;
        public final List<NetworkLocation> successorSuccessors;

        public StabilizationData(NetworkLocation successorPredecessor, List<NetworkLocation> successorSuccessors) {
            this.successorPredecessor = successorPredecessor;
            this.successorSuccessors = successorSuccessors;
        }

    }

    public HashingAlgorithm getHashingAlgorithm() {
        return this.hashingAlgorithm;
    }

    public NetworkLocation getSuccessor() {
        return this.successors.getFirst();
    }

    public List<NetworkLocation> getSuccessors(int n) {
        return this.successors.get(n);
    }

    public NetworkLocation getLocation() {
        return this.ownLocation;
    }

    public NetworkLocation getPredecessor() {
        return this.predecessor;
    }

    private void initFingerTable(NetworkLocation location) {
        BigInteger maxValue = this.hashingAlgorithm.getMax();

        for (int i = 0; i < TABLE_SIZE; i++) {
            BigInteger step = BigInteger.valueOf(2).pow(i);
            BigInteger fingerKey = this.hashingAlgorithm.hash(location).add(step).mod(maxValue);
            this.fingerTableKeys.add(fingerKey);
            this.fingerTable.put(fingerKey, location);
        }

        this.fingerTable.put(this.hashingAlgorithm.hash(this.ownLocation), location);
        this.fingerTableUpdateIndex = 0;
    }

    private Map.Entry<BigInteger, NetworkLocation> advanceUpdateIteratorIterator() {

        if (this.fingerTableUpdateIndex == this.fingerTableKeys.size()) {
            this.fingerTableUpdateIndex = 0;
        }

        BigInteger fingerKey = this.fingerTableKeys.get(this.fingerTableUpdateIndex);

        this.fingerTableUpdateIndex++;
        return new AbstractMap.SimpleEntry<>(fingerKey, this.fingerTable.get(fingerKey));
    }

    private boolean betweenTwoKeys(BigInteger lowerBound, BigInteger upperBound, BigInteger key, boolean closedLeft,
                                   boolean closedRight) {
        // TODO: Took this from my previous project, but I think Lukas already did it
        // somewhere
        // Equal to one of the bounds and are inclusive
        if ((closedLeft && key.equals(lowerBound)) || (closedRight && key.equals(upperBound)))
            return true;

        // Equal to one of the bounds and are exclusive
        if ((!closedLeft && key.equals(lowerBound)) || (!closedRight && key.equals(upperBound)))
            return false;

        // Whole circle is valid
        if (upperBound.equals(lowerBound))
            return true;

        // Not crossing
        if (lowerBound.compareTo(upperBound) < 0) {
            return (lowerBound.compareTo(key) < 0) && (key.compareTo(upperBound) < 0);
        }
        // Crossing
        else {
            return !((upperBound.compareTo(key) < 0) && (key.compareTo(lowerBound) < 0));
        }
    }

    public int getSuccessorCount() {
        return this.successors.count();
    }
    /* LISTENERS */

    public void addListener(ChordListener listener) {
        this.listeners.add(listener);
    }

    /* SYSTEM SPECIFIC */

    public boolean isWriteResponsible(String key) throws ChordException {
        return isWriteResponsible(ownLocation, key);
    }

    private boolean isWriteResponsible(NetworkLocation networkLocation, String key) throws ChordException {
        if (this.getPredecessor().equals(NetworkLocation.NULL)) {
            return this.getSuccessorCount() == 0;
        }

        return betweenTwoKeys(
                hashingAlgorithm.hash(checkNullLocation(messaging.getPredecessor(networkLocation))),
                hashingAlgorithm.hash(networkLocation),
                hashingAlgorithm.hash(key),
                false,
                true);
    }

    private static NetworkLocation checkNullLocation(NetworkLocation networkLocation) throws ChordException {
        if (networkLocation.equals(NetworkLocation.NULL))
            throw new ChordException("Supplied location '%s' was null location", networkLocation);
        return networkLocation;
    }

    public boolean isReadResponsible(String key) throws ChordException {
        if (!isReplicationActive()) return isWriteResponsible(this.ownLocation, key);

        final var replicatedLocations = getReplicatedLocations(ownLocation);
        if (replicatedLocations.isEmpty())
            throw new ChordException("Could not find replicated locations for location %s", ownLocation);

        return Try.sequence(replicatedLocations.stream()
                        .map(iterLocation -> Try.of(() -> isWriteResponsible(iterLocation, key)))
                        .collect(Collectors.toList())
                ).getOrElseThrow(throwable -> new ChordException("Could not check write responsibility of replicated " +
                        "locations", throwable))
                .exists(Boolean::booleanValue);
    }

    private boolean isReplicationActive() {
        return this.getSuccessorCount() == Constants.NUMBER_OF_REPLICAS;
    }

    private List<NetworkLocation> getReplicatedLocations(NetworkLocation networkLocation) throws ChordException {
        return Try.sequence(Stream.iterate(Try.success(networkLocation),
                                        iterTry -> iterTry.mapTry(messaging::getPredecessor)
                                                .mapTry(Chord::checkNullLocation)
                                ).limit(Constants.NUMBER_OF_REPLICAS + 1L)
                                .collect(Collectors.toList())
                ).getOrElseThrow(failCause ->
                        new ChordException("Caught exception while getting predecessors", failCause))
                .reverse().toJavaList();
    }

    public NetworkLocation getWriteResponsibleNetworkLocation(String key) throws ChordException {
        return isWriteResponsible(key) ? ownLocation : findSuccessor(hashingAlgorithm.hash(key));
    }

    public List<NetworkLocation> getReadResponsibleNetworkLocation(String key) throws ChordException {
        final NetworkLocation writeResponsibleNetworkLocation = getWriteResponsibleNetworkLocation(key);

        if (!isReplicationActive()) return new LinkedList<>(List.of(writeResponsibleNetworkLocation));

        List<NetworkLocation> result = Stream.concat(Stream.of(writeResponsibleNetworkLocation),
                        messaging.getSuccessors(writeResponsibleNetworkLocation, SUCCESSOR_LIST_SIZE).stream())
                .collect(Collectors.toList());

        return new LinkedList<>(result);
    }


    /* OTHER */
    public String getStateStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("CHORD Instance\n");
        sb.append(String.format("Own: %s (%s)%n", this.ownLocation,
                this.hashingAlgorithm.hash(this.ownLocation).toString(16)));
        sb.append(String.format("Predecessor: %s (%s)%n", this.predecessor,
                this.predecessor.equals(NetworkLocation.NULL) ? "N/A"
                        : this.hashingAlgorithm.hash(this.predecessor).toString(16)));
        sb.append("------\n");
        sb.append("Finger Table\n");

        for (int i = 0; i < this.fingerTableKeys.size(); i++) {
            BigInteger key = this.fingerTableKeys.get(i);
            NetworkLocation value = this.fingerTable.get(key);
            value = value == null ? this.ownLocation : value;
            sb.append(String.format("%s - %d (%s) %n", key.toString(16), value.getPort(),
                    hashingAlgorithm.hash(value).toString(16)));
        }
        sb.append("------\n");
        sb.append(this.successors.getStateStr());
        return sb.toString();
    }

}
