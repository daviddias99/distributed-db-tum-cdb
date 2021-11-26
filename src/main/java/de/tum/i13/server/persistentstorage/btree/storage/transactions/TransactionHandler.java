package de.tum.i13.server.persistentstorage.btree.storage.transactions;

import java.io.Serializable;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;

/**
 * A transaction handler is reponsible for transactions on the persistent
 * storage. It is meant to be able to rollback mutable operations (insert
 * deletes) done after the transaction start. The normal flow of using a
 * transaction handler should be {@code beginTransaction} 'operation'
 * {@code endTransaction} or {code rollbackTransaction} depending on the success
 * of the operation.
 */
public interface TransactionHandler<V> extends Serializable {

  /**
   * Method called when a chunk has changed.
   * 
   * @param chunkId id of the chunk that changed
   * @throws StorageException an exception is thrown when an error occurs when
   *                          trying to acess disk.
   */
  public void notifyChunkChange(String chunkId) throws StorageException;

  /**
   * Method called when a chunk has been creted.
   * 
   * @param chunkId id of the newly created chunk
   * @throws StorageException an exception is thrown when an error occurs when
   *                          trying to acess disk.
   */
  public void notifyChunkCreation(String chunkId) throws StorageException;

  /**
   * Reverts operations made since the last {@code beginTransaction} operation and
   * returns previous tree state. This state is used to update the in-memory
   * version of the tree, since the disk version is updated by this method.
   * 
   * @return Previous tree root
   * @throws StorageException an exception is thrown when an error occurs when
   *                          trying to acess disk.
   */
  public PersistentBTreeNode<V> rollbackTransaction() throws StorageException;

  /**
   * Start a new transaction. Note that the behavior of this class is undefined if
   * another transaction is started before the finishing of a previous one.
   * 
   * @throws StorageException an exception is thrown when an error occurs when
   *                          trying to acess disk.
   */
  public void beginTransaction() throws StorageException;

  /**
   * End an ongoing transaction.
   * 
   * @throws StorageException an exception is thrown when an error occurs when
   *                          trying to acess disk.
   */
  public void endTransaction() throws StorageException;
}
