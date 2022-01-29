package de.tum.i13.server.kvchord;

import java.util.List;

import de.tum.i13.shared.net.NetworkLocation;

public interface ChordListener {

  void predecessorChanged(NetworkLocation previous, NetworkLocation current);

  void successorChanged(NetworkLocation previous, NetworkLocation current);

  void successorsChanged(List<NetworkLocation> previous, List<NetworkLocation> current);
}
