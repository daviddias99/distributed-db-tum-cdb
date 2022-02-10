package de.tum.i13.server.kvchord.commandprocessing.handlers;

import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.KVChordListener;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handler that manages server shutdown (handoff).
 */
public class ShutdownHandler implements Runnable {

    private final Thread listeningThread;
    private final ChordServerState state;
    private final PersistentStorage storage;
    private final Chord chordInstance;
    private final KVChordListener changeListener;

    /**
     * Create a new shutdown handler
     *
     * @param listeningThread main server connection listening thread
     * @param chordInstance   Chord instance used by server
     * @param state           current server state
     * @param storage         current server storage
     * @param changeListener  Chord change listener (used for managing replication)
     */
    public ShutdownHandler(Thread listeningThread, Chord chordInstance, ChordServerState state,
                           PersistentStorage storage, KVChordListener changeListener) {
        this.listeningThread = listeningThread;
        this.state = state;
        this.chordInstance = chordInstance;
        this.storage = storage;
        this.changeListener = changeListener;
    }

    @Override
    public void run() {
        Logger LOGGER = LogManager.getLogger(ShutdownHandler.class);
        LOGGER.info("Starting server shutdown procedure");

        HashingAlgorithm hashing = this.chordInstance.getHashingAlgorithm();
        NetworkLocation destination = this.chordInstance.getSuccessor();
        NetworkLocation lower = this.chordInstance.getPredecessor();
        NetworkLocation upper = this.chordInstance.getLocation();

        this.state.writeLock();

        if (destination.equals(upper) || lower.equals(NetworkLocation.NULL) || destination.equals(NetworkLocation.NULL)) {
            return;
        }

        String lowerBound = hashing.hash(lower).toString(16);
        String upperBound = hashing.hash(upper).toString(16);
        LOGGER.info("Trying to execute handoff of [{}-{}]", lowerBound, upperBound);

        Runnable handoff = new HandoffHandler(destination, lowerBound, upperBound, this.storage, this.state, hashing);

        handoff.run();
        this.changeListener.deleteReplicatedRanges(false);
        LOGGER.info("Finished shutdown procedure");

        // Stop listenint thread
        listeningThread.interrupt();
    }

}
