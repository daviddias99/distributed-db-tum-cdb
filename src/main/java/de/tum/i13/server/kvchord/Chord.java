package de.tum.i13.server.kvchord;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Chord {

    private static final Logger LOGGER = LogManager.getLogger(Chord.class);

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
        this.successors = new ChordSuccessorList(SUCCESSOR_LIST_SIZE, ownLocation, fingerTable, hashingAlgorithm);
        this.messaging = new ChordMessaging(this);
        this.bootstrapNode = bootstrapNode;
        this.listeners =  new LinkedList<>();
        this.initFingerTable(ownLocation);
    }

    private void doBoostrap() throws ChordException {
        NetworkLocation successor = this.messaging.findSuccessor(bootstrapNode,
                this.hashingAlgorithm.hash(ownLocation));

        if (successor.equals(NetworkLocation.getNull())) {
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

        if (nPrimeSuccessor.equals(NetworkLocation.getNull())) {
            return new NetworkLocation[] { nPrime, nPrime }; 
        }

        while (!this.betweenTwoKeys(
                hashingAlgorithm.hash(nPrime),
                hashingAlgorithm.hash(nPrimeSuccessor),
                key,
                false,
                true)) {
            NetworkLocation newNPrime = this.messaging.closestPrecedingFinger(nPrime, key);

            if (newNPrime.equals(NetworkLocation.getNull())) {
                LOGGER.error("Could not get closest preceding finger from {}", nPrime);
                throw new ChordException("Could not get closest preceding finger");
            }

            // Avoid infinite loop
            if (newNPrime == nPrime) {
                break;
            }

            nPrime = newNPrime;
            nPrimeSuccessor = this.messaging.getSuccessor(nPrime);

            if (nPrimeSuccessor.equals(NetworkLocation.getNull())) {
                LOGGER.error("Could not get successor from {}", nPrime);
                throw new ChordException("Could not get sucessor");
            }
        }

        return new NetworkLocation[] { nPrime, nPrimeSuccessor };
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

        if (this.predecessor.equals(NetworkLocation.getNull()) || isBetweenKeys) {
            this.setPredecessor(peer);
        }
    }

    private StabilizationData querySuccessorForStabilization() {
        while (true) {
            NetworkLocation successor = this.getSuccessor();

            if(successor.equals(NetworkLocation.getNull())) {
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
        if (successor.equals(this.ownLocation) || successor.equals(NetworkLocation.getNull())) {
            if (!this.getPredecessor().equals(NetworkLocation.getNull()) && !this.getPredecessor().equals(this.ownLocation)) {
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

        if (successorPredecessor.equals(NetworkLocation.getNull())) {
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

            if (newFinger.equals(NetworkLocation.getNull())) {
                return;
            }

            this.fingerTable.put(toUpdate.getKey(), newFinger);
            LOGGER.debug("Fixed finger {} to {}", toUpdate.getKey().toString(16), newFinger);
        } catch (ChordException e) {
            LOGGER.error("Could not fix finger {}", toUpdate.getKey().toString(16));
        }
    }

    private void checkPredecessor() {
        if (this.ownLocation.equals(this.predecessor) || this.predecessor.equals(NetworkLocation.getNull())) {
            return;
        }

        boolean predecessorAlive = this.messaging.isNodeAlive(this.predecessor);

        if (!predecessorAlive) {
            this.setPredecessor(NetworkLocation.getNull());
        }
    }

    private void periodicFunction(Runnable function) {
        try {
            function.run();
        } catch (Exception e) {
            LOGGER.atError()
                    .withThrowable(e)
                    .log("Exception was caught during periodic thread execution");
        }
    }

    private void initThreads() {
        final ScheduledThreadPoolExecutor periodicThreadPool = (ScheduledThreadPoolExecutor) Executors
                .newScheduledThreadPool(3);

        Runnable t1 = () -> periodicFunction(this::stabilize);
        Runnable t2 = () -> periodicFunction(this::fixFingers);
        Runnable t3 = () -> periodicFunction(this::checkPredecessor);

        // The threads are started with a delay to avoid them running at the same time
        periodicThreadPool.scheduleWithFixedDelay(t1, 0, 1000, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleWithFixedDelay(t2, 200, 1000, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleWithFixedDelay(t3, 400, 1000, TimeUnit.MILLISECONDS);
    }

    private void setPredecessor(NetworkLocation predecessor) {
        NetworkLocation oldPredecessor = this.predecessor;
        this.predecessor = predecessor;

        for (ChordListener list : this.listeners) {
            list.predecessorChanged(oldPredecessor, predecessor);
        }
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

    public boolean isWriteResponsible(String key) {
        return isWriteResponsible(ownLocation, key);
    }

    private boolean isWriteResponsible(NetworkLocation networkLocation, String key) {
        try {
            return betweenTwoKeys(
                    hashingAlgorithm.hash(messaging.getPredecessor(networkLocation)),
                    hashingAlgorithm.hash(networkLocation),
                    hashingAlgorithm.hash(key),
                    false,
                    true);
        } catch (ChordException e) {
            LOGGER.error("Could not determine predecessor of network location {}", networkLocation);
            return false;
        }
    }

    public boolean isReadResponsible(String key) {
        // TODO Check whether replication is actually active

        if(this.getSuccessorCount() < Constants.NUMBER_OF_REPLICAS) {
            return isWriteResponsible(this.ownLocation, key);
        }

        return getReplicatedLocations(ownLocation).stream()
                .anyMatch(iterLocation -> isWriteResponsible(iterLocation, key));
    }

    private List<NetworkLocation> getReplicatedLocations(NetworkLocation networkLocation) {
        return Stream.iterate(networkLocation,
                iterLocation -> {
                    try {
                        return messaging.getPredecessor(iterLocation);
                    } catch (ChordException e) {
                        LOGGER.error("Could not find predecessor of {}", iterLocation);
                        throw new RuntimeException(e);
                    }
                })
                .limit(Constants.NUMBER_OF_REPLICAS + 1)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                collection -> {
                                    Collections.reverse(collection);
                                    return collection;
                                }));
    }

    public NetworkLocation getWriteResponsibleNetworkLocation(String key) throws ChordException {
        return isWriteResponsible(key) ? ownLocation : findSuccessor(hashingAlgorithm.hash(key));
    }

    public List<NetworkLocation> getReadResponsibleNetworkLocation(String key) throws ChordException {
        // TODO Check whether replication is actually active
        final NetworkLocation writeResponsibleNetworkLocation = getWriteResponsibleNetworkLocation(key);
        
        if(this.getSuccessorCount() < Constants.NUMBER_OF_REPLICAS) {
            return Arrays.asList(writeResponsibleNetworkLocation);
        }
        
        return Stream.concat(Stream.of(writeResponsibleNetworkLocation),
                messaging.getSuccessors(writeResponsibleNetworkLocation, SUCCESSOR_LIST_SIZE).stream())
                .collect(Collectors.toList());
    }


    /* OTHER */
    public String getStateStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("CHORD Instance\n");
        sb.append(String.format("Own: %s (%s)%n", this.ownLocation,
                this.hashingAlgorithm.hash(this.ownLocation).toString(16)));
        sb.append(String.format("Predecessor: %s (%s)%n", this.predecessor,
                this.predecessor.equals(NetworkLocation.getNull()) ? "N/A"
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
