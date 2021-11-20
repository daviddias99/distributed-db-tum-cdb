package de.tum.i13.server.persistentStorage.btree.chunk;

import java.io.Serializable;

/**
 * Data class representing a {@link Serializable} key-value pair. Keys are Strings and value types are configured through the generic {@code V}
 * @param <V> Type of vlaue
 */
public class Pair<V> implements Serializable {

  private static final long serialVersionUID = 6529685098267755681L;

  public final String key;
  public final V value;

  /**
   * Create new pair
   * @param key key
   * @param value value
   */
  public Pair(String key, V value) {
    this.key = key;
    this.value = value;
  }
}
