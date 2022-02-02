package de.tum.i13.server.persistentstorage.btree.io.chunk;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.persistentstorage.btree.io.transactions.ChangeListener;

/**
 * Handles storage of {@link Chunk}s.
 * 
 * @param <V> Type of the values used in the BTree.
 */
public interface ChunkStorageHandler<V> {

    /**
     * Read the stored chunk.
     * 
     * @return The stored chunk
     * @throws StorageException An exception is thrown when the handler wansn't able
     *                          to read the chunk
     */
    public Chunk<V> readChunk() throws StorageException;

    /**
     * Store the chunk. If the chunk is empty, the chunk is deleted instead.
     * 
     * @param chunk chunk to store
     * 
     * @throws StorageException An exception is thrown when the handler wansn't able
     *                          to store or delete the chunk
     */
    public void storeChunk(Chunk<V> chunk) throws StorageException;

    /**
     * Store the chunk. This method differs from {@link #storeChunk(Chunk)
     * storeChunk} by storing the chunk even if it's empty.
     * 
     * @param chunk chunk to create
     * 
     * 
     * @throws StorageException An exception is thrown when the handler wansn't able
     *                          to store or delete the chunk
     */
    public void createChunk(Chunk<V> chunk) throws StorageException;

    public void setListener(ChangeListener listener);
}
