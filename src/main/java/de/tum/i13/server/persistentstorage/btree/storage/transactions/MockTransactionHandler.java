package de.tum.i13.server.persistentstorage.btree.storage.transactions;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;

public class MockTransactionHandler<V> implements TransactionHandler<V> {

  public void notifyChunkChange(String chunkId) {
    // Method purposefuly left empty

  }

  public void notifyChunkCreation(String chunkId) {
    // Method purposefuly left empty
  }

  public PersistentBTreeNode<V> rollbackTransaction() {
    // Method purposefuly left empty
    return null;
  }

  public void beginTransaction() {
    // Method purposefuly left empty
  }

  public void endTransaction() {
    // Method purposefuly left empty
  }
}
