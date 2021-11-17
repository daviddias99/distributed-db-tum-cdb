package de.tum.i13.server.persistentStorage.btree.chunk;

import java.io.Serializable;

public class Pair<V> implements Serializable {

  private static final long serialVersionUID = 6529685098267755681L;

  public final String key;
  public final V value;

  public Pair(String key, V value) {
    this.key = key;
    this.value = value;
  }
}
