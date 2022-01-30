package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.kv.commandprocessing.KVServerCommandProcessor;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Command processor for KVMessages. Uses {@link KVClientCommandProcessor},
 * {@link KVServerCommandProcessor} and {@link KVChordCommandProcessor} to parse these messages.
 */
public class KVCommandProcessor implements CommandProcessor<String> {
    private static final Logger LOGGER = LogManager.getLogger(KVCommandProcessor.class);

    private final ServerState serverState;
    private final List<CommandProcessor<KVMessage>> processors;

    public KVCommandProcessor(PersistentStorage storage, ChordServerState serverState, Chord chord) {
        this.serverState = serverState;
        this.processors = Arrays.asList(
                new KVServerCommandProcessor(storage, serverState),
                new KVChordCommandProcessor(chord),
                new KVClientCommandProcessor(storage, serverState));
    }

    @Override
    public String process(String command) {
        KVMessage incomingMessage = KVMessage.unpackMessage(command);
        PeerType peerType = PeerAuthenticator.authenticate(incomingMessage.getStatus());

        if (this.serverState.isStopped() && !peerType.canBypassStop()) {
            LOGGER.warn("Can't process command '{}' because server is stopped", command);
            return new KVMessageImpl(StatusType.SERVER_STOPPED).toString();
        }

        final KVMessage response = processors.stream()
                .map(processor -> processor.process(incomingMessage))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseGet(() -> new KVMessageImpl(StatusType.ERROR));

        if(response.getStatus() != StatusType.SERVER_HEART_BEAT) {
            LOGGER.debug("Response processing '{}' -> '{}'", command, response);
        }

        return response.toString();
    }
}
