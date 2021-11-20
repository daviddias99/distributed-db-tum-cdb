package de.tum.i13.server.persistentStorage.btree;

import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.Pair;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.StorageException;

// A BTree
public class PersistentBTree<V> implements Serializable {
  private static final long serialVersionUID = 6529685098267757690L;

  PersistentBTreeNode<V> root; // Root node
  public int minimumDegree; // Minimum degree
  private PersistentBTreeStorageHandler<V> storageHandler;
  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();


  // Constructor (Initializes tree as empty)
  PersistentBTree(int minimumDegree, PersistentBTreeStorageHandler<V> storageHandler) {
    this.root = null;
    this.minimumDegree = minimumDegree;
    this.storageHandler = storageHandler;
  }

  public void remove(String key) throws StorageException {

    this.readWriteLock.writeLock().lock();

    if (root == null)
      return;

    // Call the remove function for root
    root.remove(key);

    // If the root node has 0 keys, make its first child as the new root
    // if it has a child, otherwise set root as NULL
    if (root.getElementCount() == 0) {
      root = root.isLeaf() ? null : root.getChildren().get(0);
    }

    this.readWriteLock.writeLock().unlock();
    this.storageHandler.save(this);
    return;
  }

  // function to search a key in this tree
  public V search(String key) throws StorageException {

    this.readWriteLock.readLock().lock();

    if (this.root == null) {
      this.readWriteLock.readLock().unlock();
      return null;
    }
    else {
      V searchResult = this.root.search(key);
      this.readWriteLock.readLock().unlock();
      return searchResult;
    }

  }

  // The main function that inserts a new key in this B-Tree
  public V insert(String key, V value) throws StorageException {
    this.readWriteLock.writeLock().lock();

    // If tree is empty
    if (root == null) {
      this.createRoot(key, value);
      this.storageHandler.save(this);
      this.readWriteLock.writeLock().unlock();
      return null;
    }

    V previousValue = searchAndInsert(key, value);

    if(previousValue != null) {
      this.storageHandler.save(this);
      this.readWriteLock.writeLock().unlock();
      return previousValue;
    }

    // If root is full, then tree grows in height
    if (root.isFull())
      this.insertFull(key, value);
    // If root is not full, call insertNonFull for root
    else 
      root.insertNonFull(key, value);

    this.storageHandler.save(this);
    this.readWriteLock.writeLock().unlock();

    return null;
  }

  private V searchAndInsert(String key, V value) throws StorageException {
    if (this.root == null)
      return null;
    else
      return this.root.searchAndInsert(key, value);
  }

  private void createRoot(String key, V value) {
    // Create new node
    try {
      root = new PersistentBTreeNode<V>(this.minimumDegree, true, new Pair<V>(key, value), this.storageHandler);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }

  private void insertFull(String key, V value) throws StorageException {
    // Allocate memory for new root
    PersistentBTreeNode<V> s;
    try {
      s = new PersistentBTreeNode<V>(this.minimumDegree, false, this.storageHandler);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    Chunk<V> chunk = s.getChunk();

    // Make old root as child of new root
    s.getChildren().set(0, root);

    // Split the old root and move 1 key to the new root
    s.splitChild(0, chunk);

    s.setChunk(chunk);

    // New root has two children now. Decide which of the
    // two children is going to have new key
    int i = 0;
    if (chunk.get(i).key.compareTo(key) < 0)
      i++;
    s.getChildren().get(i).insertNonFull(key, value);

    // Change root
    root = s;
  }

  public void delete() throws StorageException {
    this.storageHandler.delete();
  }
}
