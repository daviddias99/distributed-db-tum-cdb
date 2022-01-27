package de.tum.i13.server.cmdproc;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageCommandProcessor implements CommandProcessor<KVMessage> {

    private static final Logger LOGGER = LogManager.getLogger(StorageCommandProcessor.class);

    protected ServerState serverState;
    protected PersistentStorage kvStore;

    public StorageCommandProcessor(ServerState serverState, PersistentStorage storage) {
        this.serverState = serverState;
        this.kvStore = storage;
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case PUT -> this.put(command.getKey(), command.getValue());
            case DELETE -> this.delete(command.getKey());
            case GET -> this.get(command.getKey());
            default -> null;
        };
    }

    /**
     * Method to search for a key in the persistent storage.
     *
     * @param key the key to search
     * @return a KVMessage with the status of the query
     */
    protected KVMessage get(String key) {
        if (!this.serverState.responsibleForKey(key)) {
            return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        try {
            LOGGER.info("Trying to read key: {}", key);
            return kvStore.get(key);
        } catch (GetException e) {
            LOGGER.error(e);
            return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
        }
    }

    protected KVMessage put(String key, String value) {
        synchronized (this.kvStore) {
            if (!this.serverState.responsibleForKey(key)) {
                return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
            }

            if (this.serverState.canWrite()) {
                try {
                    LOGGER.info("Trying to put key: {} and value: {}", key, value);
                    return kvStore.put(key, value);
                } catch (PutException e) {
                    LOGGER.error(e);
                    return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_ERROR);
                }
            }

            return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

    }

    protected KVMessage delete(String key) {
        synchronized (this.kvStore) {

            if (!this.serverState.responsibleForKey(key)) {
                return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
            }

            if (this.serverState.canWrite()) {
                try {
                    LOGGER.info("Trying to delete key: {}", key);
                    return kvStore.put(key, null);
                } catch (PutException e) {
                    LOGGER.error(e);
                    return new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
                }
            }

            return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
        }
    }

}
