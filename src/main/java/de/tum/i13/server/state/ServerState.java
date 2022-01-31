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
     */
    boolean isWriteResponsible(String key) throws ServerException;

    /**
     * Check if a given key is the responsibility of the current server for reading
     *
     * @param key key to check
     * @return true if the server is responsible for the key
     */
    boolean isReadResponsible(String key) throws ServerException;

    List<NetworkLocation> getReadResponsibleNetworkLocation(String key);

    NetworkLocation getCurNetworkLocation();

    boolean isReplicationActive();

    void executeStoredDeletes(PersistentStorage storage);
    
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
