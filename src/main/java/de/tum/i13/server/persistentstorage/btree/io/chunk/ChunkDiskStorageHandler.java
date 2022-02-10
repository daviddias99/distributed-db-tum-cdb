package de.tum.i13.server.persistentstorage.btree.io.chunk;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.persistentstorage.btree.io.StorageUtils;
import de.tum.i13.server.persistentstorage.btree.io.transactions.ChangeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link Chunk} on disk.
 */
public class ChunkDiskStorageHandler<V> implements ChunkStorageHandler<V>, Serializable {

    private static final Logger LOGGER = LogManager.getLogger(ChunkDiskStorageHandler.class);

    private static final long serialVersionUID = 6529685098267757691L;

    private final String chunkId; // Chunk ID
    private final String storageFolder; // Storage folder

    private ChangeListener cListener;

    /**
     * Create new storage handler. This handler interfaces with a chunk at
     * {@code filePath}
     *
     * @param storageFolder path to the folder where chunks are storage
     * @param chunkId       ID of the current chunk
     * @param transHandler  transactionHandler
     */
    public ChunkDiskStorageHandler(String chunkId, String storageFolder, ChangeListener transHandler) {
        this.chunkId = chunkId;
        this.cListener = transHandler;
        this.storageFolder = storageFolder;
    }

    @Override
    public Chunk<V> readChunk() throws StorageException {
        return StorageUtils.readObject(Paths.get(storageFolder, chunkId));
    }

    @Override
    public void storeChunk(Chunk<V> chunk) throws StorageException {
        this.cListener.notifyChunkChange(chunkId);
        if (chunk.getElementCount() == 0) {
            this.deleteChunk();
            return;
        }

        StorageUtils.writeObject(Paths.get(storageFolder, chunkId), chunk);
    }

    @Override
    public void createChunk(Chunk<V> chunk) throws StorageException {
        this.cListener.notifyChunkCreation(chunkId);
        StorageUtils.writeObject(Paths.get(storageFolder, chunkId), chunk);
    }

    private void deleteChunk() throws StorageException {
        try {
            Files.delete(Paths.get(storageFolder, chunkId));
            LOGGER.debug("Deleted chunk ({}) from disk.", Paths.get(storageFolder, chunkId));
        } catch (IOException e) {
            throw new StorageException(e, "I/O error while deleting chunk from disk");
        }
    }

    @Override
    public void setListener(ChangeListener listener) {
        this.cListener = listener;
    }

}
