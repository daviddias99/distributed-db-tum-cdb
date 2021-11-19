package de.tum.i13.server.persistentStorage.btree;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeStorageHandler;

public class BTreePersistentStorage implements PersistentStorage{

  private PersistentBTree<String> tree;

  public BTreePersistentStorage(int minimumDegree, PersistentBTreeStorageHandler<String> storageHandler) {
    this.tree = new PersistentBTree<>(minimumDegree, storageHandler);
  }

  @Override
  public KVMessage get(String key) throws GetException {

    try {
      String value = this.tree.search(key);

      if (value == null) {
        return new KVMessageImpl(key, value, KVMessage.StatusType.GET_ERROR);
      }

      return new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS);
    } catch (Exception e) {
      throw new GetException("An error occured while fetching key %s from storage.", key);
    }
  }

  @Override
  public KVMessage put(String key, String value) throws PutException {

    try {
      if (value == null) {
        this.tree.remove(key);

        return new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS);
      }

      String previousValue = this.tree.insert(key, value);

      if (value != previousValue) {
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
      }

      return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
    } catch (Exception e) {
      throw new PutException("An error occured while %s key %s from storage.", value == null ? "deleting" : "putting" ,  key);
    }
  }
}
