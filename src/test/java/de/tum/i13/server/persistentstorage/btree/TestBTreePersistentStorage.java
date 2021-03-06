package de.tum.i13.server.persistentstorage.btree;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.MD5HashAlgorithm;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PutException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBTreePersistentStorage {
    private BTreePersistentStorage storage;
    private PersistentBTreeDiskStorageHandler<Pair<String>> handler;

    @BeforeAll
    public static void setLogLevel() {
        Configurator.setRootLevel(Level.INFO);
    }

    @BeforeEach
    public void createTree() throws StorageException {
        handler = new PersistentBTreeDiskStorageHandler<>("database", true);
        HashingAlgorithm hashAlg = new MD5HashAlgorithm();
        storage = new BTreePersistentStorage(3, handler, hashAlg);
    }

    @AfterEach
    public void deleteTree() throws StorageException {
        handler.delete();
    }

    @Test
    void putReturnsCorrectKeysAndValues() throws PutException {
        assertThat(storage.put("myKey", "myValue"))
                .extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("myKey", null, KVMessage.StatusType.PUT_SUCCESS);
        assertThat(storage.put("myKey2", "myValue2"))
                .extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("myKey2", null, KVMessage.StatusType.PUT_SUCCESS);
    }

    @Test
    void getReturnsCorrectKeysAndValues() throws PutException, GetException {
        storage.put("myKey", "myValue");
        storage.put("myKey2", "myValue2");
        assertThat(storage.get("myKey")).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("myKey", "myValue", KVMessage.StatusType.GET_SUCCESS);
    }

    @Test
    void deletesKey() throws PutException, GetException {

        for (int i = 0; i < 3; i++) {
            storage.put("key" + i, "value" + i);
        }

        assertThat(storage.get("key1")).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("key1", "value1", KVMessage.StatusType.GET_SUCCESS);
        assertThat(storage.put("key1", null)).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("key1", null, KVMessage.StatusType.DELETE_SUCCESS);
        assertThat(storage.get("key1")).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("key1", null, KVMessage.StatusType.GET_ERROR);
    }

    @Test
    void deletesNonExistant() throws PutException, GetException {

        for (int i = 0; i < 3; i++) {
            storage.put("key" + i, "value" + i);
        }

        assertThat(storage.put("key6", null)).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("key6", null, KVMessage.StatusType.DELETE_ERROR);
        assertThat(storage.get("key6")).extracting(KVMessage::getKey, KVMessage::getValue, KVMessage::getStatus)
                .containsExactly("key6", null, KVMessage.StatusType.GET_ERROR);
    }
}
