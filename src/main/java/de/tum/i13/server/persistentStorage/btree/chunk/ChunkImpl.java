package de.tum.i13.server.persistentStorage.btree.chunk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.tum.i13.shared.Preconditions;

/**
 * A serializable (see {@link Serializable}) implementation of {@link Chunk}.
 * Stores elements in a fixed-size ArrayList.
 */
public class ChunkImpl<V> implements Chunk<V>, Serializable {
  private List<Pair<V>> elements;
  private int minimumDegree;

  private static final long serialVersionUID = 6529685098267757681L;

  /**
   * Create a new Chunk with given minimum degree. Note that all nodes in a B-Tree
   * (except the root) must have at list {@code minimumDegree - 1} and at most
   * {@code 2*minimumDegree  - 1} elements.
   * 
   * @param minimumDegree B-Tree minimum degree
   */
  public ChunkImpl(int minimumDegree) {
    this.minimumDegree = minimumDegree;
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));
  }

  /**
   * Create a new Chunk with given minimum degree and initialized with some elements. Note that all nodes in a B-Tree
   * (except the root) must have at list {@code minimumDegree - 1} and at most
   * {@code 2*minimumDegree  - 1} elements.
   * 
   * @param minimumDegree B-Tree minimum degree
   * @param newElements List of initial elements
   */
  public ChunkImpl(int minimumDegree, List<Pair<V>> newElements) {
    Preconditions.check(newElements.size() <= 2 * minimumDegree - 1);
    this.minimumDegree = minimumDegree;
    this.elements = new ArrayList<Pair<V>>(Collections.nCopies((2 * minimumDegree - 1), null));
    Collections.copy(this.elements, newElements);
  }

  @Override
  public int findIndexOfFirstGreaterThen(String k) {
    int i = 0;
    int n = this.getElementCount();
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
  public void shiftRightOne(int startIndex) {
    int keyCount = this.getElementCount();

    for (int j = keyCount - 1; j >= startIndex; j--) {
      this.elements.set(j + 1, this.elements.get(j));
      this.elements.set(j, null);
    }
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
  public int shiftRightOneAfterFirstGreaterThan(String key) {
    int keyCount = this.getElementCount();
    int i = keyCount - 1;

    while (i >= 0 && elements.get(i).key.compareTo(key) > 0) {
      elements.set(i + 1, elements.get(i));
      elements.set(i, null);
      i--;
    }

    if (i < 0) {
      return i + 1;
    }

    return elements.get(i).key.compareTo(key) == 0 ? i : i + 1;
  }

  @Override
  public ChunkImpl<V> clone() {
    ChunkImpl<V> chunkClone = new ChunkImpl<>(this.minimumDegree, this.elements);
    return chunkClone;
  }

  @Override
  public int getElementCount() {
    int i = 0;

    for (Pair<V> pair : elements) {
      if (pair != null) {
        i += 1;
      }
    }

    return i;
  }

  @Override
  public List<Pair<V>> getElements() {
    return this.elements;
  }
}
