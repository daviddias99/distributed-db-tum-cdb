package de.tum.i13.server.persistentstorage.btree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.server.persistentstorage.btree.storage.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;
import de.tum.i13.shared.Preconditions;

/**
 * Uses a Persistent B-Tree (https://en.wikipedia.org/wiki/B-tree) implemented
 * by ({@link PersistentBTree}) to provided a {@link PersistentStorage}
 */
public class BTreePersistentStorage implements PersistentStorage {
    private static final Logger LOGGER = LogManager.getLogger(BTreePersistentStorage.class);

    private PersistentBTree<String> tree;

    /**
     * Create a new B-Tree with a given minum degree (see {@link PersistentBTree}).
     * Storage handler is also configurable.
     * 
     * @param minimumDegree  B-Tree minimum degree
     * @param storageHandler Handler used by the BTree to persist
     */
    public BTreePersistentStorage(int minimumDegree, PersistentBTreeStorageHandler<String> storageHandler) {
        try {
            this.tree = storageHandler.load();
        } catch (StorageException e) {
            // Purposefuly empty
        }

        if (this.tree == null) {
            this.tree = new PersistentBTree<>(minimumDegree, storageHandler);
        }
    }

    @Override
    public KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        try {
            String value = this.tree.search(key);

            if (value == null) {
                LOGGER.info("No value with key {}", key);
                return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
            }
            LOGGER.info("Found value {} with key {}", value, key);
            return new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS);
        } catch (Exception e) {
            throw new GetException("An error occured while fetching key %s from storage.", key);
        }
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        Preconditions.notNull(key, "Key cannot be null");

        try {
            if (value == null) {
                LOGGER.info("Trying to delete key {}", key);
                boolean deleted = this.tree.remove(key);
                LOGGER.info("Deleted key {}", key);

                return deleted ? new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS)
                        : new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
            }

            LOGGER.info("Trying to put key {} with value {}", key, value);

            String previousValue = this.tree.insert(key, value);

            // Note: this returns a PUT_SUCCESS if the value already exists but is updated
            // with the same value.
            if (!value.equals(previousValue) && previousValue != null) {
                LOGGER.info("Updated key {} with value {}", key, value);

                return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
            }

            LOGGER.info("Put key {} with value {}", key, value);

            return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
        } catch (Exception e) {
            throw new PutException("An error occured while %s key %s from storage.",
                    value == null ? "deleting" : "putting", key);
        }
    }

    /**
     * Closes tree ensuring that modifying operations (inserts and deletes) can
     * finish first.
     */
    public void close() {
        this.tree.close();
    }

    /**
     * Enables tree operations have it has been closed.
     */
    public void reopen() {
        this.tree.reopen();
    }
}
