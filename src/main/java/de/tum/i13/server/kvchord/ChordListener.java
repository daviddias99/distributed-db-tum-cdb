package de.tum.i13.server.kvchord;

import java.util.List;

import de.tum.i13.shared.net.NetworkLocation;

/**
 * Listener for Chord changes
 */
public interface ChordListener {

  /**
   * Called when Chord predecessor changes
   * @param previous previous predecessor
   * @param current current predecessor
   */
  void predecessorChanged(NetworkLocation previous, NetworkLocation current);

  /**
   * Called when Chord immediate successor changes
   * @param previous previous successor
   * @param current current successor
   */
  void successorChanged(NetworkLocation previous, NetworkLocation current);

  /**
   * Called when one or more of the successors changes
   * @param previous previous successor list
   * @param current current successor list
   */
  void successorsChanged(List<NetworkLocation> previous, List<NetworkLocation> current);
}
