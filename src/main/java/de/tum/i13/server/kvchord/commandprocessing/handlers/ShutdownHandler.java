package de.tum.i13.server.kvchord.commandprocessing.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.KVChordListener;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

/**
 * Handler that manages server shutdown (handoff).
 */
public class ShutdownHandler implements Runnable {

  private Thread listeningThread;
  private ChordServerState state;
  private PersistentStorage storage;
  private Chord chordInstance;
  private KVChordListener changeListener;


  /**
   * Create a new shutdown handler
   * 
   * @param ecsComms  ECS communications interface
   * @param processor processor of commands from the ECS
   */
  public ShutdownHandler(Thread listeningThread, Chord chordInstance, ChordServerState state, PersistentStorage storage, KVChordListener changeListener) {
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
