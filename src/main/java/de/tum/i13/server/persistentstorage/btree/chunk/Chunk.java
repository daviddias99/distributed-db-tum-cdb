package de.tum.i13.server.persistentstorage.btree.chunk;
import java.io.Serializable;
import java.util.List;


/**
 * A data-structure containing key-value pairs (see {@link Pair}) to
 * be used as data-blocks in a B-Tree. Contains operations commonly performed on
 * these data-blocks. The structure is sorted by the key values in increasing
 * order.
 * 
 * @param <V> type to be used in values
 */
public interface Chunk<V> extends Serializable{
    /**
     * Finds index that contains the first element with a key greater or equal than
     * {@code key}
     * 
     * @param key key to check
     * @return index of first element with a key greater than {@code key}
     */
    public int findIndexOfFirstGreaterOrEqualThen(String key);

    /**
     * Finds index that contains the first element with a key greater than
     * {@code key}
     * 
     * @param key key to check
     * @return index of first element with a key greater than {@code key}
     */
    public int findIndexOfFirstGreaterThen(String key);

    /**
     * Get element at {@code index}
     * 
     * @param index index of the element to return
     * @return the element at the specified position in this chunk
     */
    public Pair<V> get(int index);

    /**
     * Replaces the element at the specified position in this list with the
     * specified element
     * 
     * @param index   index of the element to replace
     * @param element element to be stored at the specified position
     * @return the element previously at the specified position
     */
    public Pair<V> set(int index, Pair<V> element);

    /**
     * Removes element at specified position in the chunk. After calling this
     * method, the {@code index} position will contain the value null.
     * 
     * @param index the index of the element to be removed
     * @return the element previously at the specified position
     */
    public Pair<V> remove(int index);

    /**
     * Shifts all the elements after {@code startIndex} right one position.
     * 
     * @param startIndex index of first element to shift right
     */
    public void shiftRightOne(int startIndex);

    /**
     * Shifts all the elements after {@code startIndex} left one position. If the
     * {@code startIndex} position contains any element it will be overriden.
     * 
     * @param startIndex index of first element to shift left
     */
    public void shiftLeftOne(int startIndex);

    /**
     * Shifts all the elements after the first element with a key larger than key
     * {@code key} right one position.
     * 
     * @param key key to check
     * @return index the chunk position where a new element should be in.
     */
    public int shiftRightOneAfterFirstGreaterThan(String key);

    /**
     * Get number of elements in chunk.
     * 
     * @return Number of elements in chunk.
     */
    public int getElementCount();

    /**
     * Get elements list. Note that some positions might be {@code null}.
     * 
     * @return list of key-value elements
     */
    public List<Pair<V>> getElements();

        /**
     * Used in hopes of allowing the garbage collector to clear the elements object.
     * This is done to remove the memory footprint of the Chunk objects. It should
     * be called once there is no expectation to acess the elements again.
     */
    public void releaseStoredElements();
}
