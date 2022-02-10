package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.commandprocessing.handlers.HandoffHandler;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.tum.i13.shared.SharedUtils.withExceptionsLogged;

/**
 * Command processor for ECS KVMessages
 */
public class KVEcsCommandProcessor implements CommandProcessor<KVMessage> {

    private static final Logger LOGGER = LogManager.getLogger(KVEcsCommandProcessor.class);

    private final ECSServerState serverState;
    private final PersistentStorage storage;
    private final ServerCommunicator ecsCommunicator;
    private final boolean asyncHandoff;

    /**
     * Create a new ECS KVMessage processor
     *
     * @param storage         server storage
     * @param serverState     server state
     * @param ecsCommunicator ECS communication interface
     * @param asyncHandoff    true if handoff is to be processed asynchronously,
     *                        false otherwise
     */
    public KVEcsCommandProcessor(PersistentStorage storage, ECSServerState serverState,
                                 ServerCommunicator ecsCommunicator,
                                 boolean asyncHandoff) {
        this.serverState = serverState;
        this.storage = storage;
        this.asyncHandoff = asyncHandoff;
        this.ecsCommunicator = ecsCommunicator;
    }

    /**
     * Create a new ECS KVMessage processor. This handler uses asynchronous
     * handoffs.
     *
     * @param storage         server storage
     * @param serverState     server state
     * @param ecsCommunicator ECS communication interface
     */
    public KVEcsCommandProcessor(PersistentStorage storage, ECSServerState serverState,
                                 ServerCommunicator ecsCommunicator) {
        this(storage, serverState, ecsCommunicator, true);
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case ECS_HEART_BEAT -> this.heartBeat();
            case ECS_WRITE_LOCK -> this.writeLock();
            case ECS_WRITE_UNLOCK -> this.writeUnlock();
            case ECS_HANDOFF -> this.handoff(command);
            case ECS_SET_KEYRANGE -> this.setKeyRange(command);
            default -> null;
        };
    }

    private KVMessage heartBeat() {
        LOGGER.trace("Acknowleging heartbeat");
        return new KVMessageImpl(KVMessage.StatusType.SERVER_HEART_BEAT);
    }

    private KVMessage writeLock() {
        LOGGER.info("Trying to change server state to write-lock");
        this.serverState.writeLock();
        return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
    }

    private KVMessage writeUnlock() {
        LOGGER.info("Trying to remove server write-lock");
        this.serverState.start();
        return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_UNLOCK);
    }

    private synchronized KVMessage setKeyRange(KVMessage command) {
        LOGGER.info("Trying set server metadata");

        ConsistentHashRing newMetadata = ConsistentHashRing.unpackMetadata(command.getKey());
        ConsistentHashRing oldMetadata = this.serverState.getRingMetadata();
        this.serverState.setRingMetadata(ConsistentHashRing.unpackMetadata(command.getKey()));
        this.serverState.handleKeyRangeChange(oldMetadata, newMetadata);

        if (this.serverState.isStopped()) {
            LOGGER.info("Trying to set server state to ACTIVE");
            this.serverState.start();
        }

        // (new Thread(withExceptionsLogged(() -> this.serverState.executeStoredDeletes(storage)))).start();

        return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
    }

    private KVMessage handoff(KVMessage command) {

        String[] bounds = command.getValue().split(" ");
        String lowerBound = bounds[0];
        String upperBound = bounds[1];

        if (bounds.length != 2) {
            LOGGER.error("More than two values given as bounds");
            return new KVMessageImpl(KVMessage.StatusType.ERROR);
        }
        LOGGER.info("Trying to execute handoff (async={}) of [{}-{}]", asyncHandoff, lowerBound, upperBound);

        NetworkLocation peerNetworkLocation = NetworkLocation.extractNetworkLocation(command.getKey());
        Runnable handoff = new HandoffHandler(peerNetworkLocation, ecsCommunicator, lowerBound, upperBound, storage,
                asyncHandoff, this.serverState, this.serverState.getRingMetadata().getHashingAlgorithm());

        if (asyncHandoff) {
            Thread handoffProcess = new Thread(withExceptionsLogged(handoff));
            handoffProcess.start();
            LOGGER.info("Started async handoff process, returing acknowlegement to ECS");
            return new KVMessageImpl(KVMessage.StatusType.SERVER_HANDOFF_ACK);
        } else {
            handoff.run();
            // Communicate sucess to ECS
            LOGGER.info("Finished sync. handoff.");
            return new KVMessageImpl(KVMessage.StatusType.SERVER_HANDOFF_SUCCESS);
        }
    }

}
