package de.tum.i13.server.persistentstorage.btree.chunk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.tum.i13.shared.Preconditions;

/**
 * An implementation of {@link Chunk} using ArrayList.
 * 
 * @param <V> type to be used in values
 */
public class ChunkImpl<V> implements Chunk<V> {
    // data stored in this chunk
    private List<Pair<V>> elements;

    private static final long serialVersionUID = 6529685098267757681L;

    /**
     * Create a new Chunk with given minimum degree. Note that all nodes in a B-Tree
     * (except the root) must have at list {@code minimumDegree - 1} and at most
     * {@code 2*minimumDegree  - 1} elements.
     * 
     * @param minimumDegree B-Tree minimum degree
     */
    public ChunkImpl(int minimumDegree) {
        this.elements = new ArrayList<>(Collections.nCopies((2 * minimumDegree - 1), null));
    }

    /**
     * Create a new chunk from another chunk
     * 
     * @param chunk chunk to clone
     */
    public ChunkImpl(Chunk<V> chunk) {
        this.elements = chunk.getElements();
    }

    /**
     * Used in hopes of allowing the garbage collector to clear the elements object.
     * This is done to remove the memory footprint of the Chunk objects. It should
     * be called once there is no expectation to acess the elements again.
     */
    public void releaseStoredElements() {
        this.elements = null;
    }

    /**
     * Create a new Chunk with given minimum degree and initialized with some
     * elements. Note that all nodes in a B-Tree (except the root) must have at list
     * {@code minimumDegree - 1} and at most {@code 2*minimumDegree  - 1} elements.
     * 
     * @param minimumDegree B-Tree minimum degree
     * @param newElements   List of initial elements
     */
    public ChunkImpl(int minimumDegree, List<Pair<V>> newElements) {
        Preconditions.check(newElements.size() <= 2 * minimumDegree - 1);
        this.elements = new ArrayList<>(Collections.nCopies((2 * minimumDegree - 1), null));
        Collections.copy(this.elements, newElements);
    }

    /**
     * Finds index that contains the first element with a key greater than
     * {@code key}
     * 
     * @param key key to check
     * @return index of first element with a key greater than {@code key}
     */
    public int findIndexOfFirstGreaterThen(String key) {
        int i = 0;
        int n = this.getElementCount();
        while (i < n && key.compareTo(elements.get(i).key) > 0)
            i++;

        return i;
    }

    /**
     * Get element at {@code index}
     * 
     * @param index index of the element to return
     * @return the element at the specified position in this chunk
     */
    public Pair<V> get(int index) {
        return this.elements.get(index);
    }

    /**
     * Replaces the element at the specified position in this list with the
     * specified element
     * 
     * @param index   index of the element to replace
     * @param element element to be stored at the specified position
     * @return the element previously at the specified position
     */
    public Pair<V> set(int index, Pair<V> element) {
        return this.elements.set(index, element);
    }

    /**
     * Removes element at specified position in the chunk. After calling this
     * method, the {@code index} position will contain the value null.
     * 
     * @param index the index of the element to be removed
     * @return the element previously at the specified position
     */
    public Pair<V> remove(int index) {
        Pair<V> elem = this.elements.get(index);
        this.elements.set(index, null);
        return elem;
    }

    /**
     * Shifts all the elements after {@code startIndex} right one position.
     * 
     * @param startIndex index of first element to shift right
     */
    public void shiftRightOne(int startIndex) {
        int keyCount = this.getElementCount();

        for (int j = keyCount - 1; j >= startIndex; j--) {
            this.elements.set(j + 1, this.elements.get(j));
            this.elements.set(j, null);
        }
    }

    /**
     * Shifts all the elements after {@code startIndex} left one position. If the
     * {@code startIndex} position contains any element it will be overriden.
     * 
     * @param startIndex index of first element to shift left
     */
    public void shiftLeftOne(int startIndex) {
        // Move all the keys after the idx-th pos one place backward
        for (int i = startIndex + 1; i < this.elements.size(); ++i) {
            this.elements.set(i - 1, this.elements.get(i));
            this.elements.set(i, null);
        }
    }

    /**
     * Shifts all the elements after the first element with a key larger than key
     * {@code key} right one position.
     * 
     * @param key key to check
     * @return index the chunk position where a new element should be in.
     */
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

    /**
     * Get number of elements in chunk.
     * 
     * @return Number of elements in chunk.
     */
    public int getElementCount() {
        int i = 0;

        for (Pair<V> pair : elements) {
            if (pair != null) {
                i += 1;
            }
        }

        return i;
    }

    /**
     * Get elements list. Note that some positions might be {@code null}.
     * 
     * @return list of key-value elements
     */
    public List<Pair<V>> getElements() {
        return this.elements;
    }
}
