package de.tum.i13.server.persistentStorage.btree.chunk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseChunk<V> implements Chunk<V>, Serializable {
  private List<Pair<V>> elements;
  private int minimumDegree;

  private static final long serialVersionUID = 6529685098267757681L;

  public DatabaseChunk(int minimumDegree) {
    // this.elements = new ArrayList<Pair<V>>(2 * minimumDegree - 1);
    this.minimumDegree = minimumDegree;
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));
  }

  public DatabaseChunk(int minimumDegree, List<Pair<V>> newElements) {
    // TODO: do check for newElements.size() >= 2 * minimumDegree - 1
    this.minimumDegree = minimumDegree;
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));

    for (int i = 0; i < newElements.size(); i++) {
      this.elements.set(i, newElements.get(i));
    }

  }

  @Override
  public int findIndexOfFirstGreaterThen(String k) {
    int i = 0;
    int n = this.getKeyCount();
    while (i < n && k.compareTo(elements.get(i).key) > 0)
      i++;

    return i;
  }

  @Override
  public Pair<V> get(int index) {
    return this.elements.get(index);
  }

  @Override
  public Pair<V> set(int index, Pair<V> element) {
    return this.elements.set(index, element);
  }

  @Override
  public Pair<V> remove(int index) {
    Pair<V> elem = this.elements.get(index);
    this.elements.set(index, null);
    return elem;
  }

  @Override
  public void shiftRightOne(int startIndex, int keyCount) {
    for (int j = keyCount - 1; j >= startIndex; j--) {
      this.elements.set(j + 1, this.elements.get(j));
      this.elements.set(j, null);
    }

    // this.keyCount--;
  }

  @Override
  public void shiftLeftOne(int startIndex) {
    // Move all the keys after the idx-th pos one place backward
    for (int i = startIndex + 1; i < this.elements.size(); ++i) {
      this.elements.set(i - 1, this.elements.get(i));
      this.elements.set(i, null);
    }
  }

  @Override
  public int shiftRightOneAfterFirstGreaterThan(String key, int keyCount) {
    int i = keyCount - 1;

    while (i >= 0 && elements.get(i).key.compareTo(key) > 0) {
      elements.set(i + 1, elements.get(i));
      elements.set(i, null);
      i--;
    }

    if (i < 0) {
      return i;
    }

    return elements.get(i).key.compareTo(key) == 0 ? i - 1 : i;
  }

  @Override
  public DatabaseChunk<V> clone(){
    DatabaseChunk<V> chunkClone = new DatabaseChunk<>(this.minimumDegree, this.elements);
    return chunkClone;
  }

  @Override
  public int getKeyCount() {
    int i = 0;

    for (Pair<V> pair : elements) {
      if(pair != null) {
        i += 1;
      }
    }

    return i;
  }

  @Override
  public List<Pair<V>> getKeys() {
    return this.elements;
  }
}
