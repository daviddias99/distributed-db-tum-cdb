package de.tum.i13.server.cmdproc;

import de.tum.i13.server.ServerException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.commandprocessing.handlers.PutDeleteReplicationHandler;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static de.tum.i13.shared.SharedUtils.withExceptionsLogged;

public class StorageCommandProcessor implements CommandProcessor<KVMessage> {

    private static final Logger LOGGER = LogManager.getLogger(StorageCommandProcessor.class);

    private final ServerState serverState;
    private final PersistentStorage kvStore;

    public StorageCommandProcessor(ServerState serverState, PersistentStorage storage) {
        this.serverState = serverState;
        this.kvStore = storage;
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case PUT -> this.put(command.getKey(), command.getValue());
            case PUT_SERVER -> this.putWithoutChecks(command.getKey(), command.getValue(), false, true);
            case PUT_SERVER_OWNER -> this.putWithoutChecks(command.getKey(), command.getValue(), true, false);
            case DELETE_SERVER -> this.putWithoutChecks(command.getKey(), null, false, true);
            case DELETE -> this.put(command.getKey(), null);
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
        try {
            if (!this.serverState.isReadResponsible(key)) {
                return new KVMessageImpl(StatusType.SERVER_NOT_RESPONSIBLE);
            }
        } catch (ServerException e) {
            LOGGER.error(e);
            return new KVMessageImpl(StatusType.ERROR);
        }

        try {
            LOGGER.info("Trying to read key: {}", key);
            return kvStore.get(key);
        } catch (GetException e) {
            LOGGER.error(e);
            return new KVMessageImpl(key, StatusType.GET_ERROR);
        }
    }

    private void replicateOperation(String key, String value) {
        List<NetworkLocation> readResponsible = this.serverState.getReadResponsibleNetworkLocation(key);

        // No need to replicate on self
        readResponsible.remove(this.serverState.getCurNetworkLocation());

        // Replicate on each successor
        for (NetworkLocation networkLocation : readResponsible) {
            LOGGER.info("Replicating '{}' to {}", key, networkLocation);
            ((new Thread(withExceptionsLogged(new PutDeleteReplicationHandler(networkLocation, key, value))))).start();
        }
    }

    protected KVMessage put(String key, String value) {
        synchronized (this.kvStore) {
            try {
                if (!this.serverState.isWriteResponsible(key))
                    return new KVMessageImpl(StatusType.SERVER_NOT_RESPONSIBLE);
            } catch (ServerException e) {
                LOGGER.error(e);
                return new KVMessageImpl(StatusType.ERROR);
            }

            if (this.serverState.canWrite()) {
                return putWithoutChecks(key, value, false, false);
            }

            return new KVMessageImpl(StatusType.SERVER_WRITE_LOCK);
        }
    }

    private KVMessage putWithoutChecks(String key, String value, boolean forceReplicate, boolean avoidReplicate) {
        if (value == null) LOGGER.info("Trying to delete key: {}", key);
        else LOGGER.info("Trying to put key: {} and value: {}", key, value);
        try {
            final KVMessage result = kvStore.put(key, value);
            if (!avoidReplicate && (forceReplicate || (this.serverState.isReplicationActive() && isSuccessfulPut(result))))
                this.replicateOperation(key, value);
            return result;
        } catch (PutException e) {
            LOGGER.error(e);
            return value == null ? new KVMessageImpl(key, StatusType.DELETE_ERROR) :
                    new KVMessageImpl(key, value, StatusType.PUT_ERROR);
        }
    }

    private boolean isSuccessfulPut(KVMessage result) {
        return switch (result.getStatus()) {
            case DELETE_SUCCESS, PUT_SUCCESS, PUT_UPDATE -> true;
            default -> false;
        };
    }

}
