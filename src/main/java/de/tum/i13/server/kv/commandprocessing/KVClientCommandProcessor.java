package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.cmdproc.StorageCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

/**
 * Command processor for client KVMessages
 */
public class KVClientCommandProcessor implements CommandProcessor<KVMessage> {

    private final StorageCommandProcessor storageCommandProcessor;
    private final HashRingCommandProcessor hashRingCommandProcessor;

    /**
     * Create a new client KVMessage processor
     *
     * @param storage     server storage
     * @param serverState server state
     */
    public KVClientCommandProcessor(PersistentStorage storage, ECSServerState serverState) {
        storageCommandProcessor = new StorageCommandProcessor(serverState, storage);
        hashRingCommandProcessor = new HashRingCommandProcessor(serverState);
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case PUT, DELETE, GET -> storageCommandProcessor.process(command);
            case KEYRANGE -> hashRingCommandProcessor.process(command);
            default -> null;
        };
    }

}
