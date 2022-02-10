package de.tum.i13.server.persistentstorage.btree.io.transactions;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;

/**
 * A transaction controller is reponsible for transactions on the persistent
 * storage. It is meant to be able to rollback mutable operations (insert
 * deletes) done after the transaction start. The normal flow of using a
 * transaction handler should be {@code beginTransaction} 'operation'
 * {@code endTransaction} or {code rollbackTransaction} depending on the success
 * of the operation.
 */
public interface TransactionController<V> {

    /**
     * Reverts operations made since the last {@code beginTransaction} operation and
     * returns previous tree state. This state is used to update the in-memory
     * version of the tree, since the disk version is updated by this method.
     *
     * @return Previous tree root
     * @throws StorageException an exception is thrown when an error occurs when
     *                          trying to acess disk.
     */
    PersistentBTreeNode<V> rollbackTransaction() throws StorageException;

    /**
     * Start a new transaction. Note that the behavior of this class is undefined if
     * another transaction is started before the finishing of a previous one.
     *
     * @throws StorageException an exception is thrown when an error occurs when
     *                          trying to acess disk.
     */
    void beginTransaction() throws StorageException;

    /**
     * End an ongoing transaction.
     *
     * @throws StorageException an exception is thrown when an error occurs when
     *                          trying to acess disk.
     */
    void endTransaction() throws StorageException;

}
