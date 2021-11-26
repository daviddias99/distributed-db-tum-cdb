package de.tum.i13.server.persistentstorage.btree.storage.chunk;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;
import de.tum.i13.server.persistentstorage.btree.storage.StorageUtils;
import de.tum.i13.server.persistentstorage.btree.storage.transactions.TransactionHandler;
import de.tum.i13.shared.Constants;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link Chunk} on disk.
 */
public class ChunkDiskStorageHandler<V> implements ChunkStorageHandler<V>, Serializable {
    private static final Logger LOGGER = LogManager.getLogger(ChunkDiskStorageHandler.class);

    private static final long serialVersionUID = 6529685098267757691L;

    private String chunkId; // Chunk ID
    private String storageFolder; // Storage folder

    private TransactionHandler<V> tHandler;

    /**
     * Create new storage handler. This handler interfaces with a chunk at
     * {@code filePath}
     * 
     * @param storageFolder path to the folder where chunks are storage
     * @param chunkId       ID of the current chunk
     * @param transHandler transactionHandler
     */
    public ChunkDiskStorageHandler(String chunkId, String storageFolder, TransactionHandler<V> transHandler) {
        this.chunkId = chunkId;
        this.tHandler = transHandler;
        this.storageFolder = storageFolder;
    }

    @Override
    public Chunk<V> readChunk() throws StorageException {
        @SuppressWarnings("unchecked")
        Chunk<V> chunk = (Chunk<V>) StorageUtils.readObject(Paths.get(storageFolder, chunkId));
        return chunk;
    }

    @Override
    public void storeChunk(Chunk<V> chunk) throws StorageException {
        this.tHandler.notifyChunkChange(chunkId);
        if (chunk.getElementCount() == 0) {
            this.deleteChunk();
            return;
        }

        this.storeChunkForce(chunk);
    }

    @Override
    public void storeChunkForce(Chunk<V> chunk) throws StorageException {
        StorageUtils.writeObject(Paths.get(storageFolder, chunkId), chunk);
    }

    @Override
    public void createChunk(Chunk<V> chunk) throws StorageException {
        this.tHandler.notifyChunkCreation(chunkId);
        this.storeChunkForce(chunk);
    }

    private void deleteChunk() throws StorageException {
        try {
            Files.delete(Paths.get(storageFolder, chunkId));
            LOGGER.debug("Deleted chunk ({}) from disk.", Paths.get(storageFolder, chunkId));
        } catch (IOException e) {
            StorageException storageException = new StorageException(e, "I/O error while deleting chunk from disk");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        }
    }
}
