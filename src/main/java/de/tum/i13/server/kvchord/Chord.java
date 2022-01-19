package de.tum.i13.server.kvchord;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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

    private final NetworkLocation ownLocation;
    private final NetworkLocation predecessor;
    private final NetworkLocation[] successors; // Maybe this and the finger table can be merged
    private final ConcurrentNavigableMap<BigInteger, NetworkLocation> fingerTable;

    public Chord(HashingAlgorithm hashingAlgorithm, NetworkLocation ownLocation) {
        this.hashingAlgorithm = hashingAlgorithm;
        this.ownLocation = ownLocation;
        this.predecessor = ownLocation; // TODO: check this
        this.TABLE_SIZE = hashingAlgorithm.getHashSizeBits();
        this.fingerTable = new ConcurrentSkipListMap<>();
        this.successors = new NetworkLocation[SUCCESSOR_LIST_SIZE];
        this.messaging = new ChordMessaging(this);

        this.initFingerTable(ownLocation);
        this.initSuccessorList(ownLocation);
    }

    public Chord(HashingAlgorithm hashingAlgorithm, NetworkLocation ownLocation, NetworkLocation bootstrapNode)
            throws ChordException {
        this(hashingAlgorithm, ownLocation);

        NetworkLocation successor = this.messaging.findSuccessor(bootstrapNode,
                this.hashingAlgorithm.hash(ownLocation));

        if (successor == null) {
            LOGGER.error("Could not boostrap chord instance with {}", bootstrapNode);
            throw new ChordException(String.format("An error occured while boostrapping Chord with %s", bootstrapNode));
        }

        this.setSuccessor(successor);
    }

    public NetworkLocation getLocation() {
        return this.ownLocation;
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
            predecessorSuccessor = this.messaging.findSuccessor(predecessor, this.hashingAlgorithm.hash(predecessor));
        }

        return new NetworkLocation[]{predecessor, predecessorSuccessor};
    }

    public NetworkLocation findSuccessor(BigInteger key) {
        return this.findPredecessor(key)[1];
    }

    // TODO: Took this from my previous project, but I think Lukas already did it
    // somewhere
    public boolean betweenTwoKeys(BigInteger lowerBound, BigInteger upperBound, BigInteger key, boolean closedLeft,
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

    NetworkLocation closestPrecedingFinger(BigInteger key) {
        Map.Entry<BigInteger, NetworkLocation> preceding = fingerTable.floorEntry(key);
        return preceding == null ? fingerTable.lastEntry().getValue() : preceding.getValue();
    }

    private NetworkLocation getSuccessor() {
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
            this.fingerTable.put(fingerKey, location);
        }

        this.fingerTable.put(this.hashingAlgorithm.hash(this.ownLocation), location);
    }

    private void initSuccessorList(NetworkLocation location) {
        Arrays.fill(this.successors, location);
    }
}
