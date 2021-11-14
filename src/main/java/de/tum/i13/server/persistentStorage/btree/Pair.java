package de.tum.i13.server.persistentStorage.btree;

import java.io.Serializable;

class Pair<V> implements Serializable {
  final String key;
  final V value;

  public Pair(String key, V value) {
    this.key = key;
    this.value = value;
  }
}
