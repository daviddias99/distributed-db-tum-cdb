package de.tum.i13.server.persistentstorage.btree.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.chunk.ChunkImpl;
import de.tum.i13.shared.Constants;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link ChunkImpl} on disk.
 */
public class ChunkDiskStorageHandler<V> implements ChunkStorageHandler<V>, Serializable {
    private static final Logger LOGGER = LogManager.getLogger(ChunkDiskStorageHandler.class);

    private static final long serialVersionUID = 6529685098267757691L;

    private String filePath; // Path to the chunk

    /**
     * Create new storage handler. This handler interfaces with a chunk at
     * {@code filePath}
     * 
     * @param filePath
     */
    public ChunkDiskStorageHandler(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public Chunk<V> readChunk() throws StorageException {

        try {
            FileInputStream fileIn = new FileInputStream(this.filePath);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);

            @SuppressWarnings("unchecked")
            ChunkImpl<V> chunk = (ChunkImpl<V>) objectIn.readObject();
            objectIn.close();
            LOGGER.trace("Loaded chunk ({}) from disk.", this.filePath);
            return chunk;

        } catch (FileNotFoundException e) {
            StorageException storageException = new StorageException(e,
                    "Throwing exception because the file %s could not be found.", this.filePath);
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        } catch (IOException e) {
            StorageException storageException = new StorageException(e, "I/O error while reading chunk from memory");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        } catch (ClassNotFoundException e) {
            StorageException storageException = new StorageException(e,
                    "Unknown error while reading chunk from memory");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        }
    }

    @Override
    public void storeChunk(Chunk<V> chunk) throws StorageException {

        if (chunk.getElementCount() == 0) {
            this.deleteChunk();
            return;
        }

        this.storeChunkForce(chunk);
    }

    @Override
    public void storeChunkForce(Chunk<V> chunk) throws StorageException {
        try {
            FileOutputStream fileOut = new FileOutputStream(this.filePath);
            var objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(chunk);
            objectOut.close();
            LOGGER.trace("Stored chunk ({}) in disk.", this.filePath);
        } catch (FileNotFoundException e) {
            StorageException storageException = new StorageException(e,
                    "Throwing exception because the file %s could not be found.", this.filePath);
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        } catch (IOException e) {
            StorageException storageException = new StorageException(e, "I/O error while writing chunk to disk");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
            throw storageException;
        }
    }

    private void deleteChunk() {
        File file = new File(this.filePath);
        file.delete();
        LOGGER.debug("Deleted chunk ({}) from disk.", this.filePath);
    }
}
