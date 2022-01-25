package de.tum.i13.server.kvchord;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;

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
    private List<NetworkLocation> successors; // Maybe this and the finger table can be merged
    private final ConcurrentNavigableMap<BigInteger, NetworkLocation> fingerTable;
    private final ArrayList<BigInteger> fingerTableKeys;

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
        this.successors = new ArrayList<>();
        this.messaging = new ChordMessaging(this);
        this.bootstrapNode = bootstrapNode;
        this.initFingerTable(ownLocation);
        this.initSuccessorList(ownLocation);
    }

    public void start() throws ChordException {
        if(bootstrapNode != null) {
            NetworkLocation successor = this.messaging.findSuccessor(bootstrapNode,
            this.hashingAlgorithm.hash(ownLocation));

            if (successor == null) {
                LOGGER.error("Could not boostrap chord instance with {}", bootstrapNode);
                throw new ChordException(String.format("An error occured while boostrapping Chord with %s", bootstrapNode));
            }

            this.setSuccessor(successor);
            messaging.notifyNode(successor);
        }
        this.initThreads();
    }

    private NetworkLocation[] findPredecessor(BigInteger key) {
        NetworkLocation predecessor = this.ownLocation;
        NetworkLocation predecessorSuccessor = this.getSuccessor();

        while (!this.betweenTwoKeys(
                hashingAlgorithm.hash(predecessor),
                hashingAlgorithm.hash(predecessorSuccessor),
                key,
                false,
                true)) {
            predecessor = this.messaging.closestPrecedingFinger(predecessor, key);
            // TODO: check for null and handle
            predecessorSuccessor = this.messaging.getSuccessor(predecessor);
        }

        return new NetworkLocation[] { predecessor, predecessorSuccessor };
    }

    public NetworkLocation findSuccessor(BigInteger key) {

        if(key.equals(this.hashingAlgorithm.hash(this.ownLocation))) {
            return this.ownLocation;
        }

        return this.findPredecessor(key)[1];
    }

    public NetworkLocation closestPrecedingFinger(BigInteger key) {
        for (BigInteger mapKey : this.fingerTable.descendingKeySet()) {
            NetworkLocation value = this.fingerTable.get(mapKey);

            if(this.betweenTwoKeys(
                this.hashingAlgorithm.hash(this.ownLocation),
                key,
                mapKey,
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

        if (this.predecessor == null || isBetweenKeys) {
            this.predecessor = peer;
        }
    }

    private void updateSuccessorList(List<NetworkLocation> successorsUpdate) {
        for (int i = 1; i < this.successors.size(); i++) {

            NetworkLocation successorUpdate = successorsUpdate.get(i - 1);
            NetworkLocation oldSuccessor = this.successors.set(i, successorUpdate);

            // TODO: this operation can be optimized
            if(!successorUpdate.equals(this.ownLocation)) {
                this.fingerTable.putIfAbsent(this.hashingAlgorithm.hash(successorUpdate), successorUpdate);
                this.fingerTable.remove(this.hashingAlgorithm.hash(oldSuccessor));
            }
        }
    }

    private class StabilizationData {
        public final NetworkLocation successorPredecessor;
        public final List<NetworkLocation> successorSuccessors;
        
        public StabilizationData (NetworkLocation successorPredecessor, List<NetworkLocation> successorSuccessors) {
            this.successorPredecessor = successorPredecessor;
            this.successorSuccessors = successorSuccessors;
        }
    }

    private StabilizationData querySuccessorForStabilization() {
        for (int i = 0; i < this.successors.size(); i++) {
            NetworkLocation successor = this.successors.get(i);
            try {
                NetworkLocation successorPredecessor = successor.equals(this.ownLocation) ? this.predecessor : messaging.getPredecessor(successor);
                List<NetworkLocation> successorSuccessors =  this.messaging.getSuccessors(successor, this.SUCCESSOR_LIST_SIZE);
                this.setSuccessor(successor);
                return new StabilizationData(successorPredecessor, successorSuccessors);
            } catch (ChordException e) {
                LOGGER.error("Failed to communicate with successor");
            }            
        }

        return null;
    }

    private void stabilize() {
        NetworkLocation successor = this.getSuccessor();

        // Check if successor is self
        if(successor.equals(this.ownLocation)) {
            if (this.getPredecessor() != null) {
                this.setSuccessor(this.getPredecessor());
            }
            return;
        }

        StabilizationData stabilizationData = this.querySuccessorForStabilization();

        if(stabilizationData == null) {
            LOGGER.error("Could not stabilize");
            return;
        }

        NetworkLocation successorPredecessor = stabilizationData.successorPredecessor;
        List<NetworkLocation> successorSuccessors =  stabilizationData.successorSuccessors;
        this.updateSuccessorList(successorSuccessors);

        // Check if successor predecessor is self
        if(this.ownLocation.equals(successorPredecessor)) {
            // No need to notify
            return;
        }
        

        boolean setNewSuccessor = this.betweenTwoKeys(
                hashingAlgorithm.hash(ownLocation),
                hashingAlgorithm.hash(successor),
                hashingAlgorithm.hash(successorPredecessor),
                false,
                false);

        if (setNewSuccessor) {
            this.setSuccessor(successorPredecessor);
        }

        messaging.notifyNode(this.getSuccessor());
    }

    private void fixFingers() {
        Map.Entry<BigInteger,NetworkLocation> toUpdate = this.advanceUpdateIteratorIterator();
        NetworkLocation newFinger = this.findSuccessor(toUpdate.getKey());
        this.fingerTable.put(toUpdate.getKey(), newFinger);
        LOGGER.debug("Fixed finger {} to {}", toUpdate.getKey().toString(16), newFinger);
    }

    private void checkPredecessor() {

        if(this.ownLocation.equals(this.predecessor)) {
            return;
        }

        boolean predecessorAlive = this.messaging.isNodeAlive(this.predecessor);

        if(!predecessorAlive) {
            this.predecessor = null;
        }
    }

    public void initThreads() {
        final ScheduledThreadPoolExecutor periodicThreadPool = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(3);

        Runnable t1 = this::stabilize;
        Runnable t2 = this::fixFingers;
        Runnable t3 = this::checkPredecessor;

        // The threads are started with a delay to avoid them running at the same time
        periodicThreadPool.scheduleAtFixedRate(t1, 0, 1000, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleAtFixedRate(t2, 200, 1000, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleAtFixedRate(t3, 400, 1000, TimeUnit.MILLISECONDS);
    }

    /* HELPER */
    public NetworkLocation getSuccessor() {
        return this.successors.get(0);
    }

    public List<NetworkLocation> getSuccessors(int n) {
        return this.successors.subList(0, Math.min(n, this.successors.size()));
    }

    private NetworkLocation setSuccessor(NetworkLocation newSuccessor) {
        NetworkLocation oldSuccessor = this.successors.set(0, newSuccessor);

        this.fingerTable.remove(this.hashingAlgorithm.hash(oldSuccessor));

        LOGGER.debug("Sucessor changed from {} to {}", oldSuccessor, newSuccessor);
        return oldSuccessor;
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

    private void initSuccessorList(NetworkLocation location) {
        this.successors = new ArrayList<>(Collections.nCopies(this.SUCCESSOR_LIST_SIZE, location));
    }


    private Map.Entry<BigInteger,NetworkLocation> advanceUpdateIteratorIterator() {

        if(this.fingerTableUpdateIndex == this.fingerTableKeys.size()) {
            this.fingerTableUpdateIndex = 0;
        }

        BigInteger fingerKey = this.fingerTableKeys.get(this.fingerTableUpdateIndex);

        this.fingerTableUpdateIndex++;
        return new AbstractMap.SimpleEntry<BigInteger, NetworkLocation>(fingerKey, this.fingerTable.get(fingerKey));
    }

    // TODO: Took this from my previous project, but I think Lukas already did it
    // somewhere
    private boolean betweenTwoKeys(BigInteger lowerBound, BigInteger upperBound, BigInteger key, boolean closedLeft,
            boolean closedRight) {

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

    public String getStateStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("CHORD Instance\n");
        sb.append(String.format("Own: %s (%s)%n", this.ownLocation, this.hashingAlgorithm.hash(this.ownLocation).toString(16)));
        sb.append(String.format("Predecessor: %s (%s)%n", this.predecessor, this.predecessor == null ? "N/A" : this.hashingAlgorithm.hash(this.predecessor).toString(16)));
        sb.append("------\n");
        sb.append("Finger Table\n");

        for (int i = 0; i < this.fingerTableKeys.size(); i++) {
            BigInteger key = this.fingerTableKeys.get(i);
            NetworkLocation value = this.fingerTable.get(key);
            value = value == null ? this.ownLocation : value;
            sb.append(String.format("%s - %d (%s) %n", key.toString(16), value.getPort(), hashingAlgorithm.hash(value).toString(16)));
        }
        sb.append("------\n");
        sb.append("Successors\n");

        for (int i = 0; i < this.successors.size(); i++) {
            NetworkLocation succ = this.successors.get(i);
            sb.append(String.format("%s - %d %n", this.hashingAlgorithm.hash(succ).toString(16), succ.getPort()));
        }

        sb.append("-----\n");
        return sb.toString();
    }
}
