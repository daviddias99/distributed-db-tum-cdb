package de.tum.i13.server.persistentstorage.btree.io;

import de.tum.i13.server.persistentstorage.btree.PersistentBTree;
import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.io.chunk.ChunkDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.chunk.ChunkStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.transactions.ChangeListener;
import de.tum.i13.server.persistentstorage.btree.io.transactions.ChangeListenerImpl;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link PersistentBTree} on disk. This implementation supports transactions.
 */
public class PersistentBTreeDiskStorageHandler<V>
        implements PersistentBTreeStorageHandler<V>, Serializable {
    private static final long serialVersionUID = 6523685098267757691L;

    private static final String DEFAULT_DIRECTORY = "bckp";

    private String storageFolder; // Tree and chunks storage folder
    private String backupFolder; // Tree and chunks storage folder
    private ChangeListener cListener;
    private boolean transactionsEnabled;
    private boolean transactionStarted; // True if a transaction has been started

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}. Note, transactions are enabled by default
     * 
     * @param storageFolder Folder where the tree and it's chunks will be
     *                      stored
     * @param reset         True if the target folder should be cleared if it
     *                      already exists.
     * @param cListener     Change listener used for transactions
     * @param backupFolder  Folder used to store chunk backups
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset, ChangeListener cListener,
            String backupFolder) throws StorageException {
        this.storageFolder = storageFolder;
        this.transactionsEnabled = true;

        if (reset) {
            StorageUtils.deleteFile(Paths.get(storageFolder));
        }
        StorageUtils.createDirectory(Paths.get(this.storageFolder));

        this.backupFolder = backupFolder;
        this.cListener = cListener;
        this.transactionStarted = false;
    }

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}.
     * 
     * @param storageFolder Folder where the tree and it's chunks will be stored
     * @param reset         True if the target folder should be cleared if it
     *                      already exists.
     * @param cListener     Change listener used for transactions
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset, ChangeListener cListener)
            throws StorageException {
        this(storageFolder, reset, cListener, DEFAULT_DIRECTORY);
    }

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}.
     * 
     * @param storageFolder Folder where the tree and it's chunks will be stored
     * @param reset         True if the target folder should be cleared if it
     *                      already exists.
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset) throws StorageException {
        this(storageFolder, reset, new ChangeListenerImpl(storageFolder), DEFAULT_DIRECTORY);
    }

    /**
     * Create a new storage handler which will store a tree in
     * {@code storageFolder}.
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

        if(!Paths.get(storageFolder, "root").toFile().exists()) {
            return null;
        }

        return StorageUtils.readObject(Paths.get(storageFolder, "root"));
    }

    @Override
    public void delete() throws StorageException {
        StorageUtils.deleteFile(Paths.get(storageFolder));
    }

    @Override
    public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) throws StorageException {
        return new ChunkDiskStorageHandler<>(chunkId, storageFolder, cListener);
    }

    @Override
    public PersistentBTreeNode<V> rollbackTransaction() throws StorageException {

        if (!transactionStarted || !transactionsEnabled) {
            return null;
        }

        // Replace changed chunks with previous versions
        for (String chunkId : this.cListener.getChangedChunks()) {
            Path src = Paths.get(this.storageFolder, this.backupFolder, chunkId);
            Path dst = Paths.get(this.storageFolder, chunkId);
            this.chunkTransfer(src, dst);
        }

        // Delete newly created chunks
        for (String chunkId : this.cListener.getCreatedChunks()) {
            try {
                Files.delete(Paths.get(this.storageFolder, chunkId));
            } catch (IOException e) {
                this.endTransaction();
                throw new StorageException(e,
                        "An error occured while deleting newly created chunk on rollback");
            }
        }

        // Replace tree structure
        this.chunkTransfer(Paths.get(this.storageFolder, this.backupFolder, "root"),
                Paths.get(this.storageFolder, "root"));
        this.endTransaction();
        // Read new root and return it

        PersistentBTreeNode<V> newRoot = StorageUtils.readObject(Paths.get(this.storageFolder, "root"));

        if (newRoot != null) {
            newRoot.setAndPropagateChangeListenerAndStorageHandler(this.cListener, this);
        }

        return newRoot;
    }

    @Override
    public void beginTransaction() throws StorageException {

        if (!transactionsEnabled) {
            return;
        }

        transactionStarted = true;

        // Create backup directory
        StorageUtils.createDirectory(Paths.get(this.storageFolder, this.backupFolder));
        // Copy tree root
        this.chunkTransfer(Paths.get(this.storageFolder, "root"),
                Paths.get(this.storageFolder, this.backupFolder, "root"));
    }

    @Override
    public void endTransaction() throws StorageException {
        if (!transactionStarted || !transactionsEnabled) {
            return;
        }

        transactionStarted = false;
        this.cListener.reset();
    }

    @Override
    public void enableTransactions() {
        this.transactionsEnabled = true;
    }

    @Override
    public void disableTransactions() {
        this.transactionsEnabled = false;
    }

    private void chunkTransfer(Path src, Path dst) throws StorageException {
        try {
            StorageUtils.copyAndReplaceFile(src, dst);

        } catch (IOException e) {
            throw new StorageException(e, "An error occured during chunk transfer");
        }
    }
}
