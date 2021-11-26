package de.tum.i13.server.persistentstorage.btree.storage;

import de.tum.i13.server.persistentstorage.btree.PersistentBTree;
import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.storage.chunk.ChunkStorageHandler;

/**
 * Handles storage of {@link PersistentBTree}s. Implementations of this
 * interface also provide factory methods for generating
 * {@link ChunkStorageHandler}s. Thus, an implementation of this interface
 * couples the Tree structure storage with the chunk storage.
 * 
 * @param <V> Type of the values used in the BTree
 */
public interface PersistentBTreeStorageHandler<V> {

    /**
     * Save the state of the {@link PersistentBTree}
     * 
     * @param tree {@link PersistentBTree} to save
     * @throws StorageException An exception is thrown when saving fails.
     */
    public void save(PersistentBTree<V> tree) throws StorageException;

    /**
     * Return saved {@link PersistentBTree}
     * 
     * @throws StorageException An exception is thrown when loading fails.
     * @return saved {@link PersistentBTree}
     */
    public PersistentBTreeNode<V> load() throws StorageException;

    /**
     * Create {@link PersistentBTree} chunk storage handler for specific chunk.
     * 
     * @param chunkId ID of the chunk handlerd by the chunk storage handler
     * @throws StorageException An exception is thrown when loading fails.
     * @return Chunk storage handler creation fails.
     */
    public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) throws StorageException;

    /**
     * Delete tree
     * 
     * @throws StorageException An exception is thrown when deletion fails.
     */
    public void delete() throws StorageException;

    public void beginTransaction() throws StorageException;

    public void endTransaction() throws StorageException;

    public PersistentBTreeNode<V> rollbackTransaction() throws StorageException;

    public void enableTransactions();

    public void disableTransactions();
}
