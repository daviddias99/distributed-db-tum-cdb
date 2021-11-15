package de.tum.i13.server.persistentStorage.btree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class DatabaseChunk<V> implements Chunk<V>, Serializable {
  private List<Pair<V>> elements;
  private int keyCount;

  public DatabaseChunk(int minimumDegree) {
    // this.elements = new ArrayList<Pair<V>>(2 * minimumDegree - 1);
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));
  }

  public DatabaseChunk(int minimumDegree, List<Pair<V>> newElements) {

    // TODO: do check for newElements.size() >= 2 * minimumDegree - 1
    this.keyCount = 1;
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));

    for (int i = 0; i < newElements.size(); i++) {
      this.elements.set(i, newElements.get(i));
    }

  }

  public DatabaseChunk(List<Pair<V>> elements) {
    this.elements = elements;
  }

  @Override
  public int findIndexOfFirstGreaterThen(String k) {
    int i = 0;
    int n = elements.size();
    while (i < n && k.compareTo(elements.get(i).key) >= 0)
      i++;

    return i;
  }

  @Override
  public Pair<V> get(int index) {
    return this.elements.get(index);
  }

  @Override
  public Pair<V> set(int index, Pair<V> element) {
    this.keyCount++;
    return this.elements.set(index, element);
  }

  @Override
  public Pair<V> remove(int index) {
    this.keyCount--;
    Pair<V> elem = this.elements.get(index);
    this.elements.set(index, null);
    return elem;
  }

  @Override
  public void shiftRightOne(int startIndex) {
    for (int j = this.keyCount - 1; j >= startIndex; j--) {
      this.elements.set(j + 1, this.elements.get(j));
    }
  }

  @Override
  public int shiftRightOneAfterFirstGreaterThan(String key) {
    int i = this.keyCount-1;

    while (i >= 0 && elements.get(i).key.compareTo(key) > 0) {
      elements.set(i + 1, elements.get(i));
      i--;
    }

    return i;
  }
}
