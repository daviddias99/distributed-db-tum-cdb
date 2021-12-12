package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Class to initiate HANDOFF of relevant key-value pairs between two servers.
 */
class ECSHandoffThread extends ECSThread {

    private static final Logger LOGGER = LogManager.getLogger(ECSHandoffThread.class);

    private final KVMessage handoffMessage;

    ECSHandoffThread(NetworkLocation successor, NetworkLocation newServer, BigInteger lowerBound,
                     BigInteger upperBound) throws IOException {
        super(successor);
        this.handoffMessage = prepareHandoffMessage(newServer, lowerBound, upperBound);
    }

    @Override
    public void run() {
        LOGGER.info("Starting handoff thread");
        try {
            LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, StatusType.ECS_WRITE_LOCK, StatusType.SERVER_WRITE_LOCK);
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_LOCK), StatusType.SERVER_WRITE_LOCK);

            LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, StatusType.ECS_HANDOFF, StatusType.SERVER_HANDOFF_SUCCESS);
            sendAndReceiveMessage(this.handoffMessage, StatusType.SERVER_HANDOFF_SUCCESS);

            LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, StatusType.ECS_WAITING_FOR_HANDOFF, StatusType.SERVER_HANDOFF_SUCCESS);
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WAITING_FOR_HANDOFF), StatusType.SERVER_HANDOFF_SUCCESS);

            LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, StatusType.ECS_WRITE_UNLOCK, StatusType.SERVER_WRITE_UNLOCK);
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_UNLOCK), StatusType.SERVER_WRITE_UNLOCK);

            // TODO Doesn't ECS_SET_KEYRANGE have to be send somewhere here?
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while reading from {}", getSocket());
        } catch (ECSException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while communication with {}", getSocket());
        }
    }

    /**
     * Prepares and returns a ECS_HANDOFF message that contains the {@link NetworkLocation} of the new server
     * and the range of keys to be sent to that server.
     *
     * @return a {@link KVMessage} to initiate handoff of key-value pairs from successor to the new server in the ring.
     */
    private KVMessage prepareHandoffMessage(NetworkLocation newServer, BigInteger lowerBound, BigInteger upperBound) {
        LOGGER.trace("Preparing the handoff message for '{}' with lower bound '{}' and upperBound '{}'",
                newServer, lowerBound, upperBound);
        String lowerHexBound = HashingAlgorithm.convertHashToHexWithPrefix(lowerBound);
        String upperHexBound = HashingAlgorithm.convertHashToHexWithPrefix(upperBound);
        String peerNetworkLocation = newServer.getAddress() + ":" + newServer.getPort();

        return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT,
                new KVMessageImpl(peerNetworkLocation, lowerHexBound + " " + upperHexBound, StatusType.ECS_HANDOFF));
    }

}
