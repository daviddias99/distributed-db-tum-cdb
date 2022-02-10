package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.cmdproc.StorageCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

/**
 * Command processor for server KVMessages
 */
public class KVServerCommandProcessor implements CommandProcessor<KVMessage> {

    private final StorageCommandProcessor storageCommandProcessor;

    /**
     * Create a new server KVMessage processor
     *
     * @param storage current server storage
     */
    public KVServerCommandProcessor(PersistentStorage storage, ServerState state) {
        this.storageCommandProcessor = new StorageCommandProcessor(state, storage);
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case PUT_SERVER, PUT_SERVER_OWNER, DELETE_SERVER -> storageCommandProcessor.process(command);
            default -> null;
        };
    }

}
