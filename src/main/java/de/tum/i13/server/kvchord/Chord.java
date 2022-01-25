package de.tum.i13.server.kvchord;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.Iterator;

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
    private final NetworkLocation[] successors; // Maybe this and the finger table can be merged
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
        this.successors = new NetworkLocation[SUCCESSOR_LIST_SIZE];
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

    public NetworkLocation getLocation() {
        return this.ownLocation;
    }

    public NetworkLocation getPredecessor() {
        return this.predecessor;
    }

    private NetworkLocation[] findPredecessor(BigInteger key) {
        NetworkLocation predecessor = this.ownLocation;
        NetworkLocation predecessorSuccessor = this.getSuccessor();

        if(predecessor.equals(this.ownLocation) && key.toString(16).equals("da")) {
            int i = 1;
        }

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
        // Map.Entry<BigInteger, NetworkLocation> preceding = fingerTable.floorEntry(key);
        // NetworkLocation preceding = null;
        
        for (BigInteger mapKey : this.fingerTable.descendingKeySet()) {
            NetworkLocation value = this.fingerTable.get(mapKey);
            // if( mapKey.compareTo(key) < 0 && !value.equals(this.ownLocation)) {
            //     preceding = value;
            //     break;
            // }

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

    private void stabilize() {
        NetworkLocation successor = this.getSuccessor();
        NetworkLocation successorPredecessor = successor.equals(this.ownLocation) ? this.predecessor : messaging.getPredecessor(successor);

        if(successorPredecessor.equals(this.ownLocation)) {
            return;
        }

        boolean isBetweenKeys = this.betweenTwoKeys(
                hashingAlgorithm.hash(ownLocation),
                hashingAlgorithm.hash(successor),
                hashingAlgorithm.hash(successorPredecessor),
                false,
                false);

        if (isBetweenKeys) {
            this.setSuccessor(successorPredecessor);
        }

        messaging.notifyNode(this.getSuccessor());
    }

    private void fixFingers() {

        if(this.ownLocation.getPort() == 25565) {
            int i = 1;
        }

        Map.Entry<BigInteger,NetworkLocation> toUpdate = this.advanceUpdateIteratorIterator();
        NetworkLocation newFinger = this.findSuccessor(toUpdate.getKey());
        this.fingerTable.put(toUpdate.getKey(), newFinger);
        LOGGER.debug("Fixed finger {} to {}", toUpdate.getKey().toString(16), newFinger);
    }

    public void initThreads() {
        final ScheduledThreadPoolExecutor periodicThreadPool = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(3);

        Runnable t1 = this::stabilize;
        Runnable t2 = this::fixFingers;

        // The threads are started with a delay to avoid them running at the same time
        periodicThreadPool.scheduleAtFixedRate(t1, 0, 1000, TimeUnit.MILLISECONDS);
        periodicThreadPool.scheduleAtFixedRate(t2, 200, 1000, TimeUnit.MILLISECONDS);
    }

    /* HELPER */
    public NetworkLocation getSuccessor() {
        return this.successors[0];
    }

    private NetworkLocation setSuccessor(NetworkLocation newSuccessor) {
        NetworkLocation oldSuccessor = this.successors[0];
        this.successors[0] = newSuccessor;
        LOGGER.debug("Sucessor changed from {} to {}", oldSuccessor, newSuccessor);
        return oldSuccessor;
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
        Arrays.fill(this.successors, location);
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
        if (upperBound.equals(lowerBound) && !key.equals(lowerBound))
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
        sb.append(String.format("Predecessor: %s (%s)%n", this.predecessor, this.hashingAlgorithm.hash(this.predecessor).toString(16)));
        sb.append("------\n");

        for (int i = 0; i < this.fingerTableKeys.size(); i++) {
            BigInteger key = this.fingerTableKeys.get(i);
            NetworkLocation value = this.fingerTable.get(key);
            value = value == null ? this.ownLocation : value;
            sb.append(String.format("%s - %d (%s) %n", key.toString(16), value.getPort(), hashingAlgorithm.hash(value).toString(16)));
        }

        sb.append("-----\n");
        return sb.toString();
    }
}
