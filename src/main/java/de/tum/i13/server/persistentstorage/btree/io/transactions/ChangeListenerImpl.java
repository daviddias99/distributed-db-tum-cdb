package de.tum.i13.server.persistentstorage.btree.io.transactions;

import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.persistentstorage.btree.io.StorageUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * An implementation of a {@link ChangeListener}
 */
public class ChangeListenerImpl implements ChangeListener {
    private static final String DEFAULT_DIRECTORY = "bckp";

    // These fields are static because, reading nodes from file leads to new
    // references being created which caused nodes to access different transaction
    // handlers. This way, the datastructure-references remain constant for all.
    private Set<String> changedChunks = new HashSet<>(); // chunks that changed since the beginning of the
                                                                // transaction
    private Set<String> createdChunks = new HashSet<>(); // chunks created since the beggining of the transaction

    private String storageFolder;
    private String backupFolder = DEFAULT_DIRECTORY;

    /**
     * Create a new transaction handler
     * 
     * @param storageFolder folder where chunks are stored
     * @param backupFolder  folder where backup chunks are stored
     */
    public ChangeListenerImpl(String storageFolder, String backupFolder) {
        this.storageFolder = storageFolder;
        this.backupFolder = backupFolder;
    }

    /**
     * Create a new transaction handler
     * 
     * @param storageFolder folder where chunks are stored
     */
    public ChangeListenerImpl(String storageFolder) {
        this(storageFolder, DEFAULT_DIRECTORY);
    }

    private void chunkTransfer(Path src, Path dst) throws StorageException {
        try {
            StorageUtils.copyAndReplaceFile(src, dst);

        } catch (IOException e) {
            throw new StorageException(e, "An error occured during chunk transfer");
        }
    }

    @Override
    public void notifyChunkChange(String chunkId) throws StorageException {
        // Make a copy if it's an existant (before the start of transaction) chunk that
        // is changing.
        if (!createdChunks.contains(chunkId) && changedChunks.add(chunkId)) {

            Path src = Paths.get(this.storageFolder, chunkId);
            Path dst = Paths.get(this.storageFolder, this.backupFolder, chunkId);
            this.chunkTransfer(src, dst);
        }
    }

    @Override
    public void notifyChunkCreation(String chunkId) {
        createdChunks.add(chunkId);
    }

    @Override
    public Set<String> getChangedChunks() {
        return this.changedChunks;
    }

    @Override
    public Set<String> getCreatedChunks() {
        return this.createdChunks;
    }

    @Override
    public void reset() {
        changedChunks = new HashSet<>();
        createdChunks = new HashSet<>();
    }
}
