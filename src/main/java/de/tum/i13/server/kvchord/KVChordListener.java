package de.tum.i13.server.kvchord;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kvchord.commandprocessing.handlers.HandoffHandler;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

public class KVChordListener implements ChordListener {

  private static final Logger LOGGER = LogManager.getLogger(KVChordListener.class);

  private ChordServerState state;
  private boolean doAsyncHandoff = false;
  private PersistentStorage storage;
  private HashingAlgorithm hashing;
  private NetworkLocation predecessorChangeMemory;

  public KVChordListener(ChordServerState state, PersistentStorage storage, HashingAlgorithm hashing) {
    this.state = state;
    this.storage = storage;
    this.hashing = hashing;
  }

  @Override
  public synchronized void predecessorChanged(NetworkLocation previous, NetworkLocation current){

    if (current.equals(NetworkLocation.getNull())) {
      this.predecessorChangeMemory = previous;
      return;
    }

    if (state.getCurNetworkLocation().equals(current) || previous.equals(NetworkLocation.getNull())) {
      return;
    }

    this.state.writeLock();
    
    String lowerBound = this.hashing.hash(previous.equals(NetworkLocation.getNull()) ? this.predecessorChangeMemory : previous).toString(16);
    String upperBound = this.hashing.hash(current).toString(16);
    LOGGER.info("Trying to execute handoff (async={}) of [{}-{}]", doAsyncHandoff, lowerBound, upperBound);
    
    Runnable handoff = new HandoffHandler(current, lowerBound, upperBound, this.storage, this.state);

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

  @Override
  public void successorsChanged(List<NetworkLocation> previous, List<NetworkLocation> current) {
    // TODO Auto-generated method stub
    
  }
}
