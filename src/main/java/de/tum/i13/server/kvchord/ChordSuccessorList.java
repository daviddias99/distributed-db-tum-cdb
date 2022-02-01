package de.tum.i13.server.kvchord;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;

class ChordSuccessorList {

  private static final Logger LOGGER = LogManager.getLogger(ChordSuccessorList.class);

  private final NetworkLocation ownLocation;
  private final List<NetworkLocation> successors;
  private final ConcurrentNavigableMap<BigInteger, NetworkLocation> fingerTable;
  private final HashingAlgorithm hashing;
  private final int tableSize;
  private final List<ChordListener> listeners;

  ChordSuccessorList(int tableSize, NetworkLocation ownLocation,
      ConcurrentNavigableMap<BigInteger, NetworkLocation> fingerTable, HashingAlgorithm hashingAlgorithm,
      List<ChordListener> listeners) {
    this.tableSize = tableSize;
    this.ownLocation = ownLocation;
    this.successors = new ArrayList<>();
    this.hashing = hashingAlgorithm;
    this.fingerTable = fingerTable;
    this.listeners = listeners;
  }

  synchronized int count() {
    return this.successors.size();
  }

  synchronized NetworkLocation getFirst() {
      return this.successors.isEmpty() ? NetworkLocation.NULL : this.successors.get(0);
  }

  synchronized List<NetworkLocation> get(int n) {
    return new LinkedList<>(this.successors.subList(0, Math.min(n, this.successors.size())));
  }

  synchronized NetworkLocation shift() {
    if (this.successors.isEmpty()) {
        return NetworkLocation.NULL;
    }

    List<NetworkLocation> oldSuccList = new LinkedList<>(this.successors);
    NetworkLocation oldSuccessor = this.successors.remove(0);
    for (ChordListener listener : this.listeners) {
      listener.successorsChanged(oldSuccList, this.successors);
    }

    this.fingerTable.remove(this.hashing.hash(this.getFirst()));
    this.fingerTable.remove(this.hashing.hash(oldSuccessor));

    return oldSuccessor;
  }

  synchronized NetworkLocation setFirst(NetworkLocation newSuccessor) {
    List<NetworkLocation> oldSuccList = new LinkedList<>(this.successors);
    if (this.successors.isEmpty()) {
      this.successors.add(newSuccessor);
      for (ChordListener listener : this.listeners) {
        listener.successorsChanged(oldSuccList, new LinkedList<>(this.successors));
      }
      return null;
    }

    NetworkLocation oldSuccessor = this.successors.set(0, newSuccessor);
    this.fingerTable.remove(this.hashing.hash(oldSuccessor));

    LOGGER.debug("Sucessor changed from {} to {}", oldSuccessor, newSuccessor);
    for (ChordListener listener : this.listeners) {
      listener.successorsChanged(oldSuccList, this.successors);
    }
    return oldSuccessor;
  }

  synchronized void update(List<NetworkLocation> successorsUpdate) {

    if (successorsUpdate.isEmpty()) {
      return;
    }

    List<NetworkLocation> testEquality = new LinkedList<>(successorsUpdate.subList(0, this.tableSize - 1));
    testEquality.remove(this.ownLocation);

    if(this.successors.subList(1, this.successors.size()).equals(testEquality)) {
      return;
    }

    List<NetworkLocation> oldSuccList = new LinkedList<>(this.successors);


    for (int i = this.successors.size() - 1; i > 0; i--) {
      NetworkLocation oldSuccessor = this.successors.remove(i);
      this.fingerTable.remove(this.hashing.hash(oldSuccessor));
    }

    for (int i = 0; i < successorsUpdate.size() && this.successors.size() != this.tableSize; i++) {
      NetworkLocation successorUpdate = successorsUpdate.get(i);
      if (successorUpdate.equals(this.ownLocation)) {
        break;
      }
      this.successors.add(successorUpdate);
      this.fingerTable.putIfAbsent(this.hashing.hash(successorUpdate), successorUpdate);
    }

    if (this.successors.isEmpty()) {
      this.successors.add(this.ownLocation);
    }

    for (ChordListener listener : this.listeners) {
      listener.successorsChanged(oldSuccList, new LinkedList<>(this.successors));
    }
  }

  synchronized String getStateStr() {

    StringBuilder sb = new StringBuilder();
    sb.append("Successors\n");

    for (int i = 0; i < this.successors.size(); i++) {
      NetworkLocation succ = this.successors.get(i);
      sb.append(String.format("%s - %d %n", this.hashing.hash(succ).toString(16), succ.getPort()));
    }

    if (this.successors.isEmpty()) {
      sb.append("No successors\n");
    }

    sb.append("-----\n");

    return sb.toString();
  }
}
