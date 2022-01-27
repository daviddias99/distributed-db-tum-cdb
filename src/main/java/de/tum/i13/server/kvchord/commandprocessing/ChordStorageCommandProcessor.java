package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.cmdproc.StorageCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

import java.util.List;
import java.util.stream.Collectors;

public class ChordStorageCommandProcessor implements CommandProcessor<KVMessage> {

    private final ChordServerState serverState;
    private final StorageCommandProcessor standardStorageCommandProcessor;

    public ChordStorageCommandProcessor(ChordServerState serverState, PersistentStorage storage) {
        this.serverState = serverState;
        this.standardStorageCommandProcessor = new StorageCommandProcessor(serverState, storage);
    }

    @Override
    public KVMessage process(KVMessage command) {
        final KVMessage standardStorageResponse = this.standardStorageCommandProcessor.process(command);
        if (standardStorageResponse.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            return switch (command.getStatus()) {
                case PUT, DELETE -> getWriteResponsibleNetworkLocations(command);
                case GET -> getReadResponsibleNetworkLocations(command);
                default -> null;
            };
        } else return standardStorageResponse;
    }

    private KVMessageImpl getReadResponsibleNetworkLocations(KVMessage command) {
        final List<NetworkLocation> networkLocations =
                serverState.getReadResponsibleNetworkLocation(command.getKey());
        return new KVMessageImpl(
                networkLocations.stream()
                        .map(NetworkLocation::toPackedString)
                        .collect(Collectors.joining(",")),
                KVMessage.StatusType.SERVER_NOT_RESPONSIBLE
        );
    }

    private KVMessageImpl getWriteResponsibleNetworkLocations(KVMessage command) {
        final NetworkLocation networkLocation =
                serverState.getWriteResponsibleNetworkLocation(command.getKey());
        return new KVMessageImpl(
                NetworkLocation.toPackedString(networkLocation),
                KVMessage.StatusType.SERVER_NOT_RESPONSIBLE
        );
    }

}
