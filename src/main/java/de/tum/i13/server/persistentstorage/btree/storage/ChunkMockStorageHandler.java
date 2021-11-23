package de.tum.i13.server.persistentstorage.btree.storage;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;

/**
 * This class provides an in-memory implementation of
 * {@link ChunkStorageHandler}. Is is mainly use for testing in order to
 * abstract from disk storage issues.
 */
public class ChunkMockStorageHandler<V> implements ChunkStorageHandler<V>, Serializable {
    private static final Logger LOGGER = LogManager.getLogger(ChunkMockStorageHandler.class);

    private static final long serialVersionUID = 6529685098267757691L;

    private Chunk<V> chunk;

    @Override
    public Chunk<V> readChunk() {
        LOGGER.trace("Loaded chunk ({}) from memory.", this.hashCode());
        return this.chunk.clone();
    }

    @Override
    public void storeChunk(Chunk<V> chunk) {
        LOGGER.trace("Stored chunk ({}) in memory.", this.hashCode());
        this.chunk = chunk;
    }

    @Override
    public void storeChunkForce(Chunk<V> chunk) {
        LOGGER.trace("Stored chunk ({}) in memory.", this.hashCode());
        this.chunk = chunk;
    }
}
