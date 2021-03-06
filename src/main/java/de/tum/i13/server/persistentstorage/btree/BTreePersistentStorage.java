package de.tum.i13.server.persistentstorage.btree;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.shared.Preconditions;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Uses a Persistent B-Tree (https://en.wikipedia.org/wiki/B-tree) implemented
 * by ({@link PersistentBTree}) to provide a {@link PersistentStorage}.
 */
public class BTreePersistentStorage implements PersistentStorage, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(BTreePersistentStorage.class);

    private PersistentBTree<Pair<String>> tree;

    private final HashingAlgorithm hashAlg;

    /**
     * Create a new B-Tree with a given minimum degree (see
     * {@link PersistentBTree}).
     * Storage handler is also configurable. First there is an attempt to load the
     * tree, if it fails, a new tree is created.
     *
     * @param minimumDegree    B-Tree minimum degree
     * @param storageHandler   Handler used by the BTree to persist
     * @param hashingAlgorithm algorithm to be use to hash data uses as key in BTree
     * @throws StorageException An exception is thrown when an error occures while
     *                          saving tree to persistent storage
     */
    public BTreePersistentStorage(int minimumDegree, PersistentBTreeStorageHandler<Pair<String>> storageHandler,
                                  HashingAlgorithm hashingAlgorithm)
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


    private String normalizeKey(String key) {
        String intermediate = this.hashAlg.hash(key).toString(16);
        return HashingAlgorithm.padLeftZeros(intermediate, this.hashAlg.getHashSizeBits() / 4);
    }

    @Override
    public KVMessage get(String key) throws GetException {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        try {
            Pair<String> keyValue = this.tree.search(this.normalizeKey(key));

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
                boolean deleted = this.tree.remove(this.normalizeKey(key));
                LOGGER.info("Deleted key {}", key);

                return deleted ? new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS)
                        : new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
            }

            LOGGER.info("Trying to put key {} with value {}", key, value);

            Pair<String> previousValue = this.tree.insert(this.normalizeKey(key), new Pair<>(key, value));

            // Note: this returns a PUT_SUCCESS if the value already exists but is updated
            // with the same value.
            if (previousValue != null && !value.equals(previousValue.value)) {
                LOGGER.info("Updated key {} with value {}", key, value);

                return new KVMessageImpl(key, KVMessage.StatusType.PUT_UPDATE);
            }

            // String tree = (new PersistentBTreeDisplay<Pair<String>>()).traverseCondensed(this.tree);
            LOGGER.info("Put key {} ({}) with value {}", key, this.normalizeKey(key), value);

            return new KVMessageImpl(key, KVMessage.StatusType.PUT_SUCCESS);
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

    @Override
    public List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {
        try {
            return this.tree.searchRange(lowerBound, upperBound).stream().map(elem -> elem.value).collect(Collectors.toList());
        } catch (Exception e) {
            throw new GetException(e, "An error occurred while fetching elements in range %s-%s from storage.",
                    lowerBound,
                    upperBound);
        }
    }

}
