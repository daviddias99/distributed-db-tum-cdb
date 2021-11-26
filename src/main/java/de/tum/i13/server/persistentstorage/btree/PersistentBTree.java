package de.tum.i13.server.persistentstorage.btree;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.storage.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Preconditions;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class represents a BTree (https://en.wikipedia.org/wiki/B-tree) that can
 * persist in memory. This implementation is also thread-safe, allowing for
 * concurrent reads. The way persistance is done is injected using a
 * {@link PersistentBTreeStorageHandler}.
 * 
 * @param <V> Type of values used in the BTree
 */
public class PersistentBTree<V> implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    private static final Logger LOGGER = LogManager.getLogger(PersistentBTree.class);

    PersistentBTreeNode<V> root; // Root node
    private int minimumDegree; // Minimum degree
    private PersistentBTreeStorageHandler<V> storageHandler;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); // used to ensure concurrent reads, and
                                                                        // exclusive
                                                                        // access writes
    private AtomicBoolean treeClosed;

    /**
     * Create a new PersistentBTree. It is possible to configure the tree's
     * minimumDegree, which gives a measure of the number of keys a node may contain
     * [minimumDegree - 1, 2 * minimumDegree - 1]. The tree is also injected with a
     * {@link PersistentBTreeStorageHandler} in order to configure storage.
     * 
     * @param minimumDegree  minimum degree of the tree (see PersistentBTree for
     *                       details)
     * @param storageHandler handler used to store the tree (used to generate the
     *                       contained chunk storage handler)
     * @throws StorageException
     */
    public PersistentBTree(int minimumDegree, PersistentBTreeStorageHandler<V> storageHandler) throws StorageException {
        Preconditions.check(minimumDegree >= 2);
        this.root = null;
        this.minimumDegree = minimumDegree;
        this.storageHandler = storageHandler;
        this.treeClosed = new AtomicBoolean(false);
        this.storageHandler.save(this);
    }

    /**
     * Create a new PersistentBTree. It is possible to configure the tree's
     * minimumDegree, which gives a measure of the number of keys a node may contain
     * [minimumDegree - 1, 2 * minimumDegree - 1]. The tree is also injected with a
     * {@link PersistentBTreeStorageHandler} in order to configure storage.
     * 
     * @param minimumDegree  minimum degree of the tree (see PersistentBTree for
     *                       details)
     * @param storageHandler handler used to store the tree (used to generate the
     *                       contained chunk storage handler)
     * @throws StorageException
     */
    public PersistentBTree(int minimumDegree, PersistentBTreeNode<V> root,
            PersistentBTreeStorageHandler<V> storageHandler) throws StorageException {
        Preconditions.check(minimumDegree >= 2);
        this.root = root;
        this.minimumDegree = minimumDegree;
        this.storageHandler = storageHandler;
        this.treeClosed = new AtomicBoolean(false);
        this.storageHandler.save(this);
    }

    /**
     * Remove element with 'key' from the tree.
     * 
     * @param key key of the element to remove
     * @return true if the value was removed, false otherwise
     * @throws StorageException         An exception is thrown if a problem occurs
     *                                  with persistent storage.
     * @throws PersistentBTreeException
     */
    public boolean remove(String key) throws StorageException, PersistentBTreeException {
        Preconditions.notNull(key);

        if (treeClosed.get()) {
            PersistentBTreeException ex = new PersistentBTreeException(
                    "Could not perform operation because tree is closed");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
            throw ex;
        }

        if (root == null)
            return false;

        this.readWriteLock.writeLock().lock();
        this.storageHandler.beginTransaction();

        try {
            // Call the remove function for root
            boolean result = root.remove(key);

            // If the root node has 0 keys, make its first child as the new root
            // if it has a child, otherwise set root as NULL
            if (root.getElementCount() == 0) {
                root = root.isLeaf() ? null : root.getChildren().get(0);
            }

            this.readWriteLock.writeLock().unlock();
            this.storageHandler.save(this);
            this.storageHandler.endTransaction();
            return result;
        } catch (StorageException e) {
            this.storageHandler.rollbackTransaction();
            throw e;
        }
    }

    /**
     * Search a key in the tree.
     * 
     * @param key key of the element to search
     * @return The value associated with the key, or {@code null} if it does not
     *         exist.
     * @throws StorageException         An exception is thrown if a problem occurs
     *                                  with persistent storage.
     * @throws PersistentBTreeException
     */
    public V search(String key) throws StorageException, PersistentBTreeException {
        Preconditions.notNull(key);

        if (treeClosed.get()) {
            PersistentBTreeException ex = new PersistentBTreeException(
                    "Could not perform operation because tree is closed");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
            throw ex;
        }

        this.readWriteLock.readLock().lock();

        if (this.root == null) {
            this.readWriteLock.readLock().unlock();
            return null;
        } else {
            V searchResult = this.root.search(key);
            this.readWriteLock.readLock().unlock();
            return searchResult;
        }
    }

    /**
     * Insert a new element into the B-Tree.
     * 
     * @param key   key to insert into B-Tree.
     * @param value value to insert into B-Tree.
     * @return Previous value or null if it does not exist.
     * @throws StorageException         An exception is thrown if a problem occurs
     *                                  with persistent storage.
     * @throws PersistentBTreeException
     */
    public V insert(String key, V value) throws StorageException, PersistentBTreeException {
        Preconditions.notNull(key);
        Preconditions.notNull(value);

        if (treeClosed.get()) {
            PersistentBTreeException ex = new PersistentBTreeException(
                    "Could not perform operation because tree is closed");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
            throw ex;
        }

        try {
            this.storageHandler.beginTransaction();
            this.readWriteLock.writeLock().lock();

            // If tree is empty
            if (root == null) {
                this.createRoot(key, value);
                this.storageHandler.save(this);
                this.readWriteLock.writeLock().unlock();
                this.storageHandler.endTransaction();
                return null;
            }

            V previousValue = searchAndInsert(key, value);

            if (previousValue != null) {
                this.storageHandler.save(this);
                this.readWriteLock.writeLock().unlock();
                this.storageHandler.endTransaction();
                return previousValue;
            }

            // If root is full, then tree grows in height
            if (root.isFull())
                this.insertFull(key, value);
            // If root is not full, call insertNonFull for root
            else
                root.insertNonFull(key, value);

            this.storageHandler.endTransaction();
            this.storageHandler.save(this);
            this.readWriteLock.writeLock().unlock();

            return null;
        } catch (StorageException ex) {
            this.root = this.storageHandler.rollbackTransaction();
            throw ex;
        }
    }

    /**
     * Delete tree from storage
     * 
     * @throws StorageException An exception is thrown if a problem occurs with
     *                          persistent storage.
     */
    public void delete() throws StorageException {
        this.storageHandler.delete();
    }

    /**
     * Closes tree ensuring that modifying operations (inserts and deletes) can
     * finish first.
     */
    public synchronized void close() {
        this.treeClosed = new AtomicBoolean(true);
        this.readWriteLock.writeLock().lock();

        LOGGER.info("Closed PersistentBTree");
    }

    /**
     * Enables tree operations have it has been closed.
     */
    public synchronized void reopen() {

        this.treeClosed = new AtomicBoolean(false);
        this.readWriteLock.writeLock().unlock();

        LOGGER.info("Reopened PersistentBTree");
    }

    /**
     * Search for an element with {@code key}. If found, replace it's value with
     * {@code value}.
     * 
     * @param key   Key to search
     * @param value Value to replace with
     * @return Previous value.
     * @throws StorageException An exception is thrown if a problem occurs with
     *                          persistent storage.
     */
    private V searchAndInsert(String key, V value) throws StorageException {
        if (this.root == null)
            return null;
        else
            return this.root.searchAndInsert(key, value);
    }

    /**
     * Create a new root with one element.
     * 
     * @param key   key of the root element
     * @param value value of the root element
     * @throws StorageException An exception is thrown if a problem occurs with
     *                          persistent storage.
     */
    private void createRoot(String key, V value) throws StorageException {
        // Create new node
        root = new PersistentBTreeNode<>(this.minimumDegree, true, new Pair<>(key, value), this.storageHandler);
    }

    /**
     * Insert key-value pair into full root.
     * 
     * @param key   key of element to insert
     * @param value value of element to insert
     * @throws StorageException An exception is thrown if a problem occurs with
     *                          persistent storage.
     */
    private void insertFull(String key, V value) throws StorageException {
        // Allocate memory for new root
        PersistentBTreeNode<V> s = new PersistentBTreeNode<>(this.minimumDegree, false, this.storageHandler);
        Chunk<V> chunk = s.getChunk();

        // Make old root as child of new root
        s.getChildren().set(0, root);

        // Split the old root and move 1 key to the new root
        s.splitChild(0, chunk);

        s.setChunk(chunk);

        // New root has two children now. Decide which of the
        // two children is going to have new key
        int i = chunk.get(0).key.compareTo(key) < 0 ? 1 : 0;

        s.getChildren().get(i).insertNonFull(key, value);

        // Change root
        root = s;
    }

    /**
     * Get minimum degree associated with Tree.
     * 
     * @return minimum degree
     */
    public int getMininumDegree() {
        return this.minimumDegree;
    }

    /**
     * 
     * Get the tree's root
     * @return tree's root
     */
    public PersistentBTreeNode<V> getRoot() {
        return this.root;
    }

    /**
     * 
     * Set the tree's root
     * @param root tree's root
     */ 
    public void setRoot(PersistentBTreeNode<V>  root) {
        this.root = root;
    }
}
