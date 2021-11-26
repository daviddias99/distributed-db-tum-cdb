package de.tum.i13.server.persistentstorage.btree.io;

import de.tum.i13.server.persistentstorage.btree.PersistentBTree;
import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.io.chunk.ChunkStorageHandler;

/**
 * Handles storage of {@link PersistentBTree}s. Implementations of this
 * interface also provide factory methods for generating
 * {@link ChunkStorageHandler}s. Thus, an implementation of this interface
 * couples the Tree structure storage with the chunk storage. This interface
 * also supports transactions {@link de.tum.i13.server.persistentstorage.btree.io.transactions.TransactionHandler}.
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
     * Return saved {@link PersistentBTree}'s root
     * 
     * @throws StorageException An exception is thrown when loading fails.
     * @return saved {@link PersistentBTree} tree root ({@link PersistentBTreeNode})
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

    /**
     * Start a transaction. Note that starting another transaction before ending a
     * previous one has undefined behavior.
     * 
     * @throws StorageException An exception is thrown when transaction start fails
     *                          due to problems with accessing disk.
     */
    public void beginTransaction() throws StorageException;

    /**
     * Finish a transaction (should be called after a call to
     * {@code beginTransaction}). Note that this method only works if transactions
     * are enabled.
     * 
     * @throws StorageException An exception is thrown when transaction end fails
     *                          due to problems with accessing disk.
     */
    public void endTransaction() throws StorageException;

    /**
     * Rollback insert/delete operations performed after the last
     * {@code beginTransaction} call. Note that this method only works if
     * transactions are enabled.
     * 
     * @return new root (previous tree structure)
     * 
     * @throws StorageException An exception is thrown when transaction end fails
     *                          due to problems with accessing disk.
     */
    public PersistentBTreeNode<V> rollbackTransaction() throws StorageException;

    /** Enable transactions */
    public void enableTransactions();

    /** Disable transactions */
    public void disableTransactions();
}
