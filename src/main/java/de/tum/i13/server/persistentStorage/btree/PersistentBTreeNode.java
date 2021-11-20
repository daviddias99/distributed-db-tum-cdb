package de.tum.i13.server.persistentStorage.btree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.ChunkImpl;
import de.tum.i13.server.persistentStorage.btree.chunk.Pair;
import de.tum.i13.server.persistentStorage.btree.storage.ChunkStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.StorageException;

/**
 * Node of a {@link PersistentBTree}
 */
class PersistentBTreeNode<V> implements Serializable {

  int minimumDegree; // Minimum degree (defines the range for number of keys), see PersistentBTree.
  List<PersistentBTreeNode<V>> children; // List of children
  int elementCount; // Current number of elements
  boolean leaf; // Is true when node is leaf. Otherwise false

  int id; // Node ID (used for storage and display purposes)
  ChunkStorageHandler<V> chunkStorageInterface; // Handle chunk storing
  PersistentBTreeStorageHandler<V> treeStorageInterface; // Used to generate chunk storage handler

  /**
   * Creates a B-Tree node with the specified parameters. This also creates a new
   * persistent Chunk using the handler provided by the tree persistent storage
   * implementation
   * 
   * @param minimumDegree      minimum degree of the tree (see PersistentBTree for
   *                           details)
   * @param leaf               true if the current node is a leaf node, i.e.
   *                           doesn't have any children
   * @param initialElement     initial element to add to the node (root)
   * @param treeStorageHandler handler used to store the tree (used to generate
   *                           the contained chunk storage handler)
   * @throws StorageException Is thrown when there is an error while creating the
   *                          contained chunk
   */
  PersistentBTreeNode(int minimumDegree, boolean leaf, Pair<V> initialElement,
      PersistentBTreeStorageHandler<V> treeStorageHandler) throws StorageException {
    this.minimumDegree = minimumDegree;
    this.children = new ArrayList<PersistentBTreeNode<V>>(Collections.nCopies((2 * minimumDegree), null));
    this.leaf = leaf;

    // Storage
    this.id = this.hashCode();
    this.chunkStorageInterface = treeStorageHandler.createChunkStorageHandler(Integer.toString(this.id));
    this.treeStorageInterface = treeStorageHandler;

    // Create chunk
    ChunkImpl<V> newChunk = initialElement == null ? new ChunkImpl<V>(minimumDegree)
        : new ChunkImpl<V>(minimumDegree, Arrays.asList(initialElement));

    this.elementCount = initialElement == null ? 0 : 1;

    // Force is used because chunk may be empty if initial element is null
    this.setChunkForce(newChunk);
  }

  /**
   * Creates a B-Tree node with the specified parameters. This also creates a new
   * persistent Chunk using the handler provided by the tree persistent storage
   * implementation.
   * 
   * @param minimumDegree      minimum degree of the tree (see PersistentBTree for
   *                           details)
   * @param leaf               true if the current node is a leaf node, i.e.
   *                           doesn't have any children
   * @param treeStorageHandler handler used to store the tree (used to generate
   *                           the contained chunk storage handler)
   * @throws StorageException Is thrown when there is an error while creating the
   *                          contained chunk
   */
  PersistentBTreeNode(int t, boolean leaf, PersistentBTreeStorageHandler<V> treeStorageHandler)
      throws StorageException {
    this(t, leaf, null, treeStorageHandler);
  }

  /**
   * Search a key in the subtree rooted with this node.
   * 
   * @param key key to search
   * @return The value associated with {@code key} or null if it is not present.
   * @throws StorageException Is thrown when there is an error while reading the
   *                          contained chunk
   */
  private V search(String key, boolean insert, V value) throws StorageException {
    Chunk<V> chunk = this.getChunk();

    // Find the first key greater than or equal to key
    int i = chunk.findIndexOfFirstGreaterThen(key);

    if (i < this.elementCount) {
      Pair<V> pair = chunk.get(i);
      // If the found key is equal to key, return this node
      if (pair != null && pair.key.equals(key)) {
        if (insert) {
          chunk.set(i, new Pair<>(key, value));
          this.setChunk(chunk);
        }
      
        return pair.value;
      }
    }

    // If the key is not found here and this is a leaf node
    if (leaf == true)
      return null;

    // Remove chunk from memory
    chunk = null;

    // Go to the appropriate child
    return children.get(i).search(key, insert, value);
  }

  V search(String key) throws StorageException {
    return this.search(key, false, null);
  }

  V searchAndInsert(String key, V value) throws StorageException {
    return this.search(key, true, value);
  }

  // A utility function to insert a new key in this node
  // The assumption is, the node must be non-full when this
  // function is called
  void insertNonFull(String key, V value) throws StorageException {
    // Initialize index as index of rightmost element
    int i = elementCount - 1;

    Chunk<V> chunk = this.getChunk();

    // If this is a leaf node
    if (leaf == true) {
      // The following loop does two things
      // a) Finds the location of new key to be inserted
      // b) Moves all greater keys to one place ahead

      i = chunk.shiftRightOneAfterFirstGreaterThan(key);

      // Insert the new key at found location
      chunk.set(i + 1, new Pair<V>(key, value));

      this.setChunk(chunk);
      this.setElementCount(chunk.getElementCount());
      chunk = null;
    } else // If this node is not leaf
    {

      // Chunk<V> chunk = this.getChunk();

      // Find the child which is going to have the new key
      while (i >= 0 && chunk.get(i).key.compareTo(key) > 0)
        i--;

      PersistentBTreeNode<V> child = this.children.get(i + 1);

      // See if the found child is full
      if (child.isFull()) {
        // If the child is full, then split it
        splitChild(i + 1, child, chunk);

        // After split, the middle key of C[i] goes up and
        // C[i] is splitted into two. See which of the two
        // is going to have the new key
        // chunk = this.getChunk();

        if (chunk.get(i + 1).key.compareTo(key) < 0)
          i++;

        this.setChunk(chunk);
      }
      chunk = null;
      this.children.get(i + 1).insertNonFull(key, value);
    }
  }

  // A utility function to split the child y of this node
  // Note that y must be full when this function is called
  void splitChild(int i, PersistentBTreeNode<V> child, Chunk<V> parentChunk) throws StorageException {
    // Create a new node which is going to store (t-1) keys
    // of y
    PersistentBTreeNode<V> newNode;
    try {
      newNode = new PersistentBTreeNode<V>(child.minimumDegree, child.leaf, this.treeStorageInterface);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    newNode.elementCount = this.minimumDegree - 1;

    Chunk<V> newNodeChunk = newNode.getChunk();
    Chunk<V> childChunk = child.getChunk();

    // Copy the last (t-1) keys of y to z
    for (int j = 0; j < this.minimumDegree - 1; j++) {
      newNodeChunk.set(j, childChunk.remove(j + this.minimumDegree));
    }

    newNode.setChunk(newNodeChunk);

    newNodeChunk = null;

    // Copy the last t children of y to z
    if (child.leaf == false) {
      for (int j = 0; j < this.minimumDegree; j++) {
        newNode.children.set(j, child.children.get(j + this.minimumDegree));
        child.children.set(j + this.minimumDegree, null);
      }
    }

    // Reduce the number of keys in y
    child.elementCount = this.minimumDegree - 1;

    // Since this node is going to have a new child,
    // create space of new child
    for (int j = this.elementCount; j >= i + 1; j--)
      this.children.set(j + 1, this.children.get(j));

    // Link the new child to this node
    this.children.set(i + 1, newNode);

    // A key of y will move to this node. Find the location of
    // new key and move all greater keys one space ahead
    parentChunk.shiftRightOne(i);

    // Copy the middle key of y to this node
    parentChunk.set(i, childChunk.remove(this.minimumDegree - 1));

    child.setChunk(childChunk);

    // Increment count of keys in this node
    this.elementCount++;
  }

  // A utility function that returns the index of the first key that is
  // greater than or equal to k
  private int findKey(String key) throws StorageException {

    Chunk<V> chunk = this.getChunk();

    int idx = 0;
    while (idx < this.elementCount && chunk.get(idx).key.compareTo(key) < 0)
      ++idx;
    return idx;
  }

  // A function to remove the key k from the sub-tree rooted with this node
  void remove(String key) throws StorageException {
    int idx = findKey(key);

    Chunk<V> chunk = this.getChunk();

    // The key to be removed is present in this node
    if (idx < this.elementCount && chunk.get(idx).key.compareTo(key) == 0) {

      // If the node is a leaf node - removeFromLeaf is called
      // Otherwise, removeFromNonLeaf function is called
      if (leaf)
        removeFromLeaf(idx);
      else
        removeFromNonLeaf(idx);
    } else {

      // If this node is a leaf node, then the key is not present in tree
      if (leaf) {
        return;
      }

      // The key to be removed is present in the sub-tree rooted with this node
      // The flag indicates whether the key is present in the sub-tree rooted
      // with the last child of this node
      boolean flag = ((idx == this.elementCount) ? true : false);

      List<PersistentBTreeNode<V>> children = this.getChildren();

      // If the child where the key is supposed to exist has less that t keys,
      // we fill that child
      if (children.get(idx).getElementCount() < this.minimumDegree)
        fill(idx);

      // If the last child has been merged, it must have merged with the previous
      // child and so we recurse on the (idx-1)th child. Else, we recurse on the
      // (idx)th child which now has atleast t keys
      if (flag && idx > this.elementCount)
        children.get(idx - 1).remove(key);
      else
        children.get(idx).remove(key);
    }
    return;
  }

  // A function to remove the idx-th key from this node - which is a leaf node
  private void removeFromLeaf(int idx) throws StorageException {

    Chunk<V> chunk = this.getChunk();

    // Move all the keys after the idx-th pos one place backward

    if (idx < this.elementCount - 1) {
      chunk.shiftLeftOne(idx);
    } else {
      chunk.set(this.elementCount - 1, null);
    }

    this.setChunk(chunk);

    // Reduce the count of keys
    this.elementCount--;

    return;
  }

  // A function to remove the idx-th key from this node - which is a non-leaf node
  private void removeFromNonLeaf(int idx) throws StorageException {

    Chunk<V> chunk = this.getChunk();

    Pair<V> k = chunk.get(idx);

    // If the child that precedes k (C[idx]) has atleast t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    if (this.getChildren().get(idx).getElementCount() >= this.minimumDegree) {
      Pair<V> pred = getPred(idx);
      chunk.set(idx, pred);
      this.getChildren().get(idx).remove(pred.key);
      this.setChunk(chunk);
    }

    // If the child C[idx] has less that t keys, examine C[idx+1].
    // If C[idx+1] has atleast t keys, find the successor 'succ' of k in
    // the subtree rooted at C[idx+1]
    // Replace k by succ
    // Recursively delete succ in C[idx+1]
    else if (this.getChildren().get(idx + 1).getElementCount() >= this.minimumDegree) {
      Pair<V> succ = getSucc(idx);
      chunk.set(idx, succ);
      this.getChildren().get(idx + 1).remove(succ.key);
      this.setChunk(chunk);
    }

    // If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
    // into C[idx]
    // Now C[idx] contains 2t-1 keys
    // Free C[idx+1] and recursively delete k from C[idx]
    else {
      merge(idx);
      this.getChildren().get(idx).remove(k.key);
    }

    return;
  }

  // A function to get predecessor of keys[idx]
  private Pair<V> getPred(int idx) throws StorageException {
    // Keep moving to the right most node until we reach a leaf
    PersistentBTreeNode<V> cur = this.getChildren().get(idx);

    while (!cur.isLeaf())
      cur = cur.getChildren().get(cur.getElementCount());

    // Return the last key of the leaf
    return cur.getChunk().get(cur.getElementCount() - 1);
  }

  private Pair<V> getSucc(int idx) throws StorageException {

    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
    PersistentBTreeNode<V> cur = this.getChildren().get(idx + 1);

    while (!cur.isLeaf())
      cur = cur.getChildren().get(0);

    // Return the first key of the leaf
    return cur.getChunk().get(0);
  }

  // A function to fill child C[idx] which has less than t-1 keys
  private void fill(int idx) throws StorageException {

    // If the previous child(C[idx-1]) has more than t-1 keys, borrow a key
    // from that child
    if (idx != 0 && this.getChildren().get(idx - 1).getElementCount() >= this.minimumDegree)
      borrowFromPrev(idx);

    // If the next child(C[idx+1]) has more than t-1 keys, borrow a key
    // from that child
    else if (idx != this.getElementCount() && this.getChildren().get(idx + 1).getElementCount() >= this.minimumDegree)
      borrowFromNext(idx);

    // Merge C[idx] with its sibling
    // If C[idx] is the last child, merge it with with its previous sibling
    // Otherwise merge it with its next sibling
    else {
      if (idx != this.getElementCount())
        merge(idx);
      else
        merge(idx - 1);
    }
    return;
  }

  // A function to borrow a key from C[idx-1] and insert it
  // into C[idx]
  private void borrowFromPrev(int idx) throws StorageException {

    PersistentBTreeNode<V> child = this.getChildren().get(idx);
    PersistentBTreeNode<V> sibling = this.getChildren().get(idx - 1);

    // The last key from C[idx-1] goes up to the parent and key[idx-1]
    // from parent is inserted as the first key in C[idx]. Thus, the loses
    // sibling one key and child gains one key

    Chunk<V> childChunk = child.getChunk();

    // Moving all key in C[idx] one step ahead
    for (int i = child.getElementCount() - 1; i >= 0; --i) {
      childChunk.set(i + 1, childChunk.get(i));
    }

    Chunk<V> chunk = this.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // If C[idx] is not a leaf, move all its child pointers one step ahead
    if (!child.isLeaf()) {
      for (int i = child.getElementCount(); i >= 0; --i)
        child.getChildren().set(i + 1, child.getChildren().get(i));
    }

    // Setting child's first key equal to keys[idx-1] from the current node
    childChunk.set(0, chunk.get(idx - 1));

    // Moving sibling's last child as C[idx]'s first child
    if (!child.isLeaf()) {
      child.getChildren().set(0, sibling.getChildren().get(sibling.elementCount));
      sibling.getChildren().set(sibling.elementCount, null);
    }

    // Moving the key from the sibling to the parent
    // This reduces the number of keys in the sibling
    chunk.set(idx - 1, siblingChunk.get(sibling.elementCount - 1));
    siblingChunk.set(sibling.elementCount - 1, null);

    child.incrementElementCount();
    sibling.decrementElementCount();

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);

    return;
  }

  // A function to borrow a key from the C[idx+1] and place
  // it in C[idx]
  private void borrowFromNext(int idx) throws StorageException {

    PersistentBTreeNode<V> child = this.children.get(idx);
    PersistentBTreeNode<V> sibling = this.children.get(idx + 1);

    Chunk<V> chunk = this.getChunk();
    Chunk<V> childChunk = child.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // keys[idx] is inserted as the last key in C[idx]
    childChunk.set(child.getElementCount(), chunk.get(idx));

    // Sibling's first child is inserted as the last child
    // into C[idx]
    if (!(child.isLeaf()))
      child.getChildren().set(child.getElementCount() + 1, sibling.getChildren().get(0));

    // The first key from sibling is inserted into keys[idx]
    chunk.set(idx, siblingChunk.get(0));

    // Moving all keys in sibling one step behind
    siblingChunk.shiftLeftOne(0);
    // for (int i = 1; i < sibling.keyCount; ++i) {
    // siblingChunk.set(i - 1, siblingChunk.get(i));
    // }

    // Moving the child pointers one step behind
    if (!sibling.leaf) {
      for (int i = 1; i <= sibling.elementCount; ++i) {
        sibling.getChildren().set(i - 1, sibling.getChildren().get(i));
        sibling.getChildren().set(i, null);
      }
    }

    // Increasing and decreasing the key count of C[idx] and C[idx+1]
    // respectively

    child.incrementElementCount();
    sibling.decrementElementCount();

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);

    return;
  }

  // A function to merge C[idx] with C[idx+1]
  // C[idx+1] is freed after merging
  private void merge(int idx) throws StorageException {
    PersistentBTreeNode<V> child = this.getChildren().get(idx);
    PersistentBTreeNode<V> sibling = this.getChildren().get(idx + 1);

    Chunk<V> chunk = this.getChunk();
    Chunk<V> childChunk = child.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // Pulling a key from the current node and inserting it into (t-1)th
    // position of C[idx]
    childChunk.set(this.minimumDegree - 1, chunk.get(idx));
    chunk.set(idx, null);

    // Copying the keys from C[idx+1] to C[idx] at the end
    for (int i = 0; i < sibling.getElementCount(); ++i) {
      childChunk.set(i + this.minimumDegree, siblingChunk.get(i));
      siblingChunk.set(i, null);
    }

    // Copying the child pointers from C[idx+1] to C[idx]
    if (!child.isLeaf()) {
      for (int i = 0; i <= sibling.getElementCount(); ++i) {
        child.getChildren().set(i + this.minimumDegree, sibling.getChildren().get(i));
      }
    }

    // Moving all keys after idx in the current node one step before -
    // to fill the gap created by moving keys[idx] to C[idx]
    for (int i = idx + 1; i < this.getElementCount(); ++i) {
      chunk.set(i - 1, chunk.get(i));
      chunk.set(i, null);
    }

    // Moving the child pointers after (idx+1) in the current node one
    // step before
    for (int i = idx + 2; i <= this.elementCount; ++i) {
      this.getChildren().set(i - 1, this.getChildren().get(i));
      this.getChildren().set(i, null);
    }

    if (idx + 2 > this.elementCount) {
      this.getChildren().set(this.elementCount, null);
    }

    // Updating the key count of child and the current node
    child.setElementCount(child.getElementCount() + sibling.getElementCount() + 1);
    this.decrementElementCount();

    // Freeing the memory occupied by sibling

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);
    return;
  }

  /**
   * Check if the node is a leaf node.
   * 
   * @return True if node is leaf false otherwise
   */
  boolean isLeaf() {
    return this.leaf;
  }

  /**
   * Check if node is full (number of elements is equal to 2 * minimumDegree - 1)
   * 
   * @return true if node is full, false otherwise
   */
  boolean isFull() {
    return this.getElementCount() == 2 * this.minimumDegree - 1;
  }

  /**
   * Get number of children in node
   * 
   * @return number of children in node
   */
  int getChildrenCount() {
    int i = 0;
    for (PersistentBTreeNode<V> persistentBTreeNode : children) {
      if (persistentBTreeNode != null) {
        i++;
      }
    }

    return i;
  }

  List<PersistentBTreeNode<V>> getChildren() {
    return this.children;
  }

  int getElementCount() {
    return this.elementCount;
  }

  /**
   * Increase element count by 1
   * 
   * @return new element count
   */
  private int incrementElementCount() {
    this.elementCount++;
    return this.elementCount;
  }

  /**
   * Decrease element count by 1
   * 
   * @return new element count
   */
  private int decrementElementCount() {
    this.elementCount--;
    return this.elementCount;
  }

  /**
   * Set new element count
   * 
   * @param newElementCount new element count
   * @return new element count (same as parameter)
   */
  private int setElementCount(int newElementCount) {
    this.elementCount = newElementCount;
    return this.elementCount;
  }

  Chunk<V> getChunk() throws StorageException {
    return chunkStorageInterface.readChunk();
  }

  /**
   * Stores given chunk in memory. The chunk is deleted if it has no elements.
   * 
   * @param chunk Chunk to store
   * @throws StorageException Throws an exception if some error occured while
   *                          storing chunk in memory
   */
  void setChunk(Chunk<V> chunk) throws StorageException {
    this.chunkStorageInterface.storeChunk(chunk);
  }

  /**
   * Stores given chunk in memory.
   * 
   * @param chunk Chunk to store
   * @throws StorageException Throws an exception if some error occured while
   *                          storing chunk in memory
   */
  void setChunkForce(Chunk<V> chunk) throws StorageException {
    this.chunkStorageInterface.storeChunkForce(chunk);
  }
}
