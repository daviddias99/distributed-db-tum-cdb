package de.tum.i13.server.persistentstorage.btree.storage.transactions;

import java.io.Serializable;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;

public interface TransactionHandler<V> extends Serializable {

  public void notifyChunkChange(String chunkId) throws StorageException;

  public void notifyChunkCreation(String chunkId) throws StorageException;

  public PersistentBTreeNode<V> rollbackTransaction() throws StorageException;

  public void beginTransaction() throws StorageException;

  public void endTransaction() throws StorageException;
}
