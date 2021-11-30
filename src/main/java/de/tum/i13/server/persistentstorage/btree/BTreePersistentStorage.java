package de.tum.i13.server.persistentstorage.btree;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.shared.Preconditions;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.MD5HashAlgorithm;

/**
 * Uses a Persistent B-Tree (https://en.wikipedia.org/wiki/B-tree) implemented
 * by ({@link PersistentBTree}) to provided a {@link PersistentStorage}
 */
public class BTreePersistentStorage implements PersistentStorage, AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(BTreePersistentStorage.class);

    private PersistentBTree<Pair<String>> tree;

    private HashingAlgorithm hashAlg;

    /**
     * Create a new B-Tree with a given minum degree (see {@link PersistentBTree}).
     * Storage handler is also configurable. First there is an attempt to load the
     * tree, if it fails, a new tree is created.
     * 
     * @param minimumDegree  B-Tree minimum degree
     * @param storageHandler Handler used by the BTree to persist
     * @throws StorageException An exception is thrown when an error occures while
     *                          saving tree to persistent storage
     */
    public BTreePersistentStorage(int minimumDegree, PersistentBTreeStorageHandler<Pair<String>> storageHandler, HashingAlgorithm hashingAlgorithm)
            throws StorageException {
        try {
            this.tree = new PersistentBTree<>(minimumDegree, storageHandler.load(), storageHandler);
        } catch (StorageException e) {
            // Purposefuly empty
        }

        if (this.tree == null) {
            this.tree = new PersistentBTree<>(minimumDegree, storageHandler);
        }

        this.hashAlg = hashingAlgorithm;
    }

    public BTreePersistentStorage(int minimumDegree, PersistentBTreeStorageHandler<Pair<String>> storageHandler)
            throws StorageException {
        this(minimumDegree, storageHandler, new MD5HashAlgorithm());
    }

    @Override
    public KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        try {
            Pair<String> keyValue = this.tree.search(this.hashAlg.hash(key).toString(16));

            if (keyValue == null) {
                LOGGER.info("No value with key {}", key);
                return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
            }
            LOGGER.info("Found value {} with key {}", keyValue.value, key);
            return new KVMessageImpl(keyValue.key, keyValue.value, KVMessage.StatusType.GET_SUCCESS);
        } catch (Exception e) {
            throw new GetException("An error occured while fetching key %s from storage.", key);
        }
    }

    @Override
    public synchronized KVMessage put(String key, String value) throws PutException {
        Preconditions.notNull(key, "Key cannot be null");

        try {
            if (value == null) {
                LOGGER.info("Trying to delete key {}", key);
                boolean deleted = this.tree.remove(this.hashAlg.hash(key).toString(16));
                LOGGER.info("Deleted key {}", key);

                return deleted ? new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS)
                        : new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
            }

            LOGGER.info("Trying to put key {} with value {}", key, value);

            Pair<String> previousValue = this.tree.insert(this.hashAlg.hash(key).toString(16), new Pair<>(key, value));

            // Note: this returns a PUT_SUCCESS if the value already exists but is updated
            // with the same value.
            if (previousValue != null && !value.equals(previousValue.value)) {
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
    @Override
    public synchronized void close() {
        this.tree.close();
    }

    /**
     * Enables tree operations have it has been closed.
     */
    public synchronized void reopen() {
        this.tree.reopen();
    }
}
