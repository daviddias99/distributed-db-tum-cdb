package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;

import java.util.Arrays;
import java.util.List;

public class KVCommandProcessor implements CommandProcessor<String> {

    private ServerState serverState;
    private List<CommandProcessor<KVMessage>> processors;

    public KVCommandProcessor(PersistentStorage storage, ServerState serverState, ServerCommunicator ecsCommunicator){
        this.serverState = serverState;
        this.processors = Arrays.asList(
            new KVServerCommandProcessor(storage),
            new KVEcsCommandProcessor(storage, serverState, ecsCommunicator),
            new KVClientCommandProcessor(storage, serverState)
        );
    }

    @Override
    public String process(String command, PeerType peerType) {
        if (this.serverState.isStopped() && !peerType.canBypassStop()) {
            return new KVMessageImpl(StatusType.SERVER_STOPPED).toString();
        }

        KVMessage incomingMessage = KVMessage.unpackMessage(command);
        KVMessage response = null;

        for (CommandProcessor<KVMessage> processor : processors) {
            response = processor.process(incomingMessage, peerType);

            if(response != null) {
                break;
            }
        }

        response = response == null ? new KVMessageImpl(KVMessage.StatusType.ERROR) : response;

        return response.toString();
    }
}
