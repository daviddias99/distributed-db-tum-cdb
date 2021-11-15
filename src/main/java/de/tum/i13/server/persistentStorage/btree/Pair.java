package de.tum.i13.server.persistentStorage.btree;

import java.io.Serializable;

class Pair<V> implements Serializable {

  private static final long serialVersionUID = 6529685098267755681L;

  final String key;
  final V value;

  public Pair(String key, V value) {
    this.key = key;
    this.value = value;
  }
}
