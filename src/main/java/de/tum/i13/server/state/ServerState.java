package de.tum.i13.server.state;

import de.tum.i13.server.ServerException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

import java.util.List;

/**
 * Class that represents the server's state. It contains the actual state and potentially additional information
 */
public interface ServerState {

    /**
     * Get the server's current state
     *
     * @return current state
     */
    State getState();

    /**
     * Check if the server is stopped
     *
     * @return true if the server is stopped
     */
    boolean isStopped();

    /**
     * Check if the server is on shutdown mode
     *
     * @return true if the server is on shutdown mode
     */
    boolean isShutdown();

    /**
     * Check if the server is active
     *
     * @return true if the server is currently active
     */
    boolean isActive();

    /**
     * Check if the server is not write locked
     *
     * @return true if the server is not write locked
     */
    boolean canWrite();

    /**
     * Set the server state to stopped
     */
    void stop();

    /**
     * Write lock the server
     */
    void writeLock();

    /**
     * Set the server to shutdown mode
     */
    void shutdown();

    /**
     * Set the server to active mode
     */
    void start();

    /**
     * Check if a given key is the responsibility of the current server for writing
     *
     * @param key key to check
     * @return true if the server is responsible for the key
     * 
     * @throws ServerException exception is thrown when the operation can't be completed due to an unforceen error
     */
    boolean isWriteResponsible(String key) throws ServerException;

    /**
     * Check if a given key is the responsibility of the current server for reading
     *
     * @param key key to check
     * @return true if the server is responsible for the key
     * 
     * @throws ServerException exception is thrown when the operation can't be completed due to an unforceen error
     */
    boolean isReadResponsible(String key) throws ServerException;

    /**
     * Get locations responsible for a given key
     * @param key key to check
     * @return list of responsible {@link NetworkLocation}s
     */
    List<NetworkLocation> getReadResponsibleNetworkLocation(String key);

    /**
     * Get network location associated with current node
     * @return network location of current node
     */
    NetworkLocation getCurNetworkLocation();

    /**
     * Check if there are enough servers to perform replication
     * @return true if replication is active
     */
    boolean isReplicationActive();

    /**
     * Executed queued delete operations
     * @param storage   {@link PersistentStorage} where delete operations are to be executed
     */
    void executeStoredDeletes(PersistentStorage storage);
    
    /**
     * Get queued delete operations
     * @return list of queued delete operations
     */
    List<String> getDeleteQueue();
    
    /**
     * Enum that represents the server's state
     */
    enum State {

        /**
         * Server is active
         */
        ACTIVE,

        /**
         * Server is stopped
         */
        STOPPED,

        /**
         * Server is doing shutdown
         */
        SHUTDOWN,

        /**
         * Server is write locked
         */
        WRITE_LOCK
    }

}
