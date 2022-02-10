package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * Command processor for KVMessages. Uses {@link KVClientCommandProcessor},
 * {@link KVServerCommandProcessor} and {@link KVEcsCommandProcessor} to parse these messages.
 */
public class KVCommandProcessor implements CommandProcessor<String> {

    private static final Logger LOGGER = LogManager.getLogger(KVCommandProcessor.class);

    private final ServerState serverState;
    private final List<CommandProcessor<KVMessage>> processors;

    /**
     * Create a new KVMessage command processor
     *
     * @param storage         server storage
     * @param serverState     server state
     * @param ecsCommunicator ECS communication interface
     */
    public KVCommandProcessor(PersistentStorage storage, ECSServerState serverState,
                              ServerCommunicator ecsCommunicator) {
        this.serverState = serverState;
        this.processors = Arrays.asList(
                new KVServerCommandProcessor(storage, serverState),
                new KVEcsCommandProcessor(storage, serverState, ecsCommunicator, false),
                new KVClientCommandProcessor(storage, serverState));
    }

    public KVCommandProcessor(PersistentStorage storage, ECSServerState serverState) {
        this.serverState = serverState;
        this.processors = Arrays.asList(
                new KVServerCommandProcessor(storage, serverState),
                new KVClientCommandProcessor(storage, serverState));
    }

    @Override
    public String process(String command) {
        KVMessage incomingMessage = KVMessage.unpackMessage(command);
        PeerType peerType = PeerAuthenticator.authenticate(incomingMessage.getStatus());

        if (this.serverState.isStopped() && !peerType.canBypassStop()) {
            LOGGER.info("Can't process command '{}' because server is stopped", command);
            return new KVMessageImpl(StatusType.SERVER_STOPPED).toString();
        }

        KVMessage response = null;

        for (CommandProcessor<KVMessage> processor : processors) {
            response = processor.process(incomingMessage);

            if (response != null) {
                break;
            }
        }

        response = response == null ? new KVMessageImpl(KVMessage.StatusType.ERROR) : response;

        if (response.getStatus() != StatusType.SERVER_HEART_BEAT) {
            LOGGER.info("Response processing '{}' -> '{}'", command, response);
        }

        return response.toString();
    }

}
