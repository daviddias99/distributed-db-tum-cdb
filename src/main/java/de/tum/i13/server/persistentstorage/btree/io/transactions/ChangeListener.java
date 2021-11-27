package de.tum.i13.server.persistentstorage.btree.io.transactions;

import java.io.Serializable;
import java.util.Set;

import de.tum.i13.server.persistentstorage.btree.io.StorageException;

/**
 * A change listener listens for chunk changed or created events. It gets
 * notified of these events through calls to the notify methods
 */
public interface ChangeListener extends Serializable {

    /**
     * Method called when a chunk has changed.
     * 
     * @param chunkId id of the chunk that changed
     * @throws StorageException an exception is thrown when an error occurs when
     *                          trying to acess disk.
     */
    public void notifyChunkChange(String chunkId) throws StorageException;

    /**
     * Method called when a chunk has been creted.
     * 
     * @param chunkId id of the newly created chunk
     * @throws StorageException an exception is thrown when an error occurs when
     *                          trying to acess disk.
     */
    public void notifyChunkCreation(String chunkId) throws StorageException;

    /**
     * Get list of changed chunks
     * 
     * @return list of changed chunks
     */
    public Set<String> getChangedChunks();

    /**
     * Get list of created chunks
     * 
     * @return list of created chunks
     */
    public Set<String> getCreatedChunks();

    public void reset();
}
