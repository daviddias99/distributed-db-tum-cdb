package de.tum.i13.server.persistentstorage.btree.storage;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.PersistentBTree;
import de.tum.i13.shared.Constants;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link PersistentBTree} on disk.
 */
public class PersistentBTreeDiskStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {
    private static final Logger LOGGER = LogManager.getLogger(PersistentBTreeDiskStorageHandler.class);
    private static final long serialVersionUID = 6523685098267757691L;

    private String storageFolder; // Tree and chunks storage folder
    private TransactionHandler tHandler;

    /**
     * Create a new storage handler which will store a tree in {@code storageFolder}
     * 
     * @param storageFolder Folder where the tree and it's chunks will be stored
     * @param reset         True if the target folder should be cleared if it
     *                      already exists.
     * @throws StorageException An exception is thrown when an error with the
     *                          {@code storageFolder} occurs
     */
    public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset) throws StorageException {
        this.storageFolder = storageFolder;
        this.tHandler = new TransactionHandler(this.storageFolder);

        if (reset) {
            StorageUtils.deleteDirectory(new File(storageFolder));
        }
        StorageUtils.createDirectory(Paths.get(this.storageFolder));
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
        StorageUtils.writeObject(Paths.get(storageFolder, "root"), tree);
    }

    @Override
    public PersistentBTree<V> load() throws StorageException {
        @SuppressWarnings("unchecked")
        PersistentBTree<V> tree = (PersistentBTree<V>) StorageUtils.readObject(Paths.get(storageFolder, "root"));
        return tree;
    }

    @Override
    public void delete() throws StorageException {
        if (!StorageUtils.deleteDirectory(new File(storageFolder))) {
            StorageException storageException = new StorageException("Unknown error while reading tree from memory");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        }
    }

    @Override
    public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) throws StorageException {
        return new ChunkDiskStorageHandler<>(chunkId, storageFolder, tHandler);
    }

    @Override
    public void beginTransaction() throws StorageException {
        this.tHandler.beginTransaction();
    }

    @Override
    public void endTransaction() {
        this.tHandler.endTransaction();
    }

    @Override
    public void rollbackTransaction() throws StorageException {
        this.tHandler.beginTransaction();
    }
}
