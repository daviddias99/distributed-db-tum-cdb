package de.tum.i13.server.persistentstorage.btree.io;

import java.io.Serializable;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.PersistentBTree;
import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.io.chunk.ChunkDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.chunk.ChunkStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.transactions.TransactionHandler;
import de.tum.i13.server.persistentstorage.btree.io.transactions.TransactionHandlerImpl;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link PersistentBTree} on disk. This implementation supports transactions
 * (see {@link TransactionHandler})
 */
public class PersistentBTreeDiskStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {
    private static final long serialVersionUID = 6523685098267757691L;

    private String storageFolder; // Tree and chunks storage folder
    private TransactionHandler<V> tHandler;
    private boolean transactionsEnabled;

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}. Note, transactions are enabled by default
     * 
     * @param storageFolder      Folder where the tree and it's chunks will be
     *                           stored
     * @param reset              True if the target folder should be cleared if it
     *                           already exists.
     * @param transactionHandler Transaction handler to be used by the storage
     *                           handler
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset,
            TransactionHandler<V> transactionHandler) throws StorageException {
        this.storageFolder = storageFolder;
        this.tHandler = transactionHandler;
        this.transactionsEnabled = true;

        if (reset) {
            StorageUtils.deleteFile(Paths.get(storageFolder));
        }
        StorageUtils.createDirectory(Paths.get(this.storageFolder));
    }

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}. A {@link TransactionHandlerImpl} is used for
     * transactions.
     * 
     * @param storageFolder Folder where the tree and it's chunks will be stored
     * @param reset         True if the target folder should be cleared if it
     *                      already exists.
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset) throws StorageException {
        this(storageFolder, reset, new TransactionHandlerImpl<>(storageFolder));
    }

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}. A {@link TransactionHandlerImpl} is used for
     * transactions.
     * 
     * @param storageFolder Folder where the tree and it's chunks will be stored
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder) throws StorageException {
        this(storageFolder, false);
    }

    @Override
    public void save(PersistentBTree<V> tree) throws StorageException {
        this.saveTreeToDisk(tree);
    }

    private void saveTreeToDisk(PersistentBTree<V> tree) throws StorageException {
        StorageUtils.writeObject(Paths.get(storageFolder, "root"), tree.getRoot());
    }

    @Override
    public PersistentBTreeNode<V> load() throws StorageException {
        @SuppressWarnings("unchecked")
        PersistentBTreeNode<V> root = (PersistentBTreeNode<V>) StorageUtils
                .readObject(Paths.get(storageFolder, "root"));
        return root;
    }

    @Override
    public void delete() throws StorageException {
        StorageUtils.deleteFile(Paths.get(storageFolder));
    }

    @Override
    public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) throws StorageException {
        return new ChunkDiskStorageHandler<>(chunkId, storageFolder, tHandler);
    }

    @Override
    public void beginTransaction() throws StorageException {
        if (!this.transactionsEnabled) {
            return;
        }
        this.tHandler.beginTransaction();
    }

    @Override
    public void endTransaction() throws StorageException {
        if (!this.transactionsEnabled) {
            return;
        }
        this.tHandler.endTransaction();
    }

    @Override
    public PersistentBTreeNode<V> rollbackTransaction() throws StorageException {
        if (!this.transactionsEnabled) {
            return null;
        }
        return this.tHandler.rollbackTransaction();
    }

    @Override
    public void enableTransactions() {
        this.transactionsEnabled = true;
    }

    @Override
    public void disableTransactions() {
        this.transactionsEnabled = false;
    }
}
