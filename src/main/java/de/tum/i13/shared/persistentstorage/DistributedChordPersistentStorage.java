package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * A {@link WrappingPersistentStorage} that is aware of the distributed nature of the
 * {@link DistributedChordPersistentStorage}.
 * Uses a {@link ConsistentHashRing} to store server metadata.
 */
public class DistributedChordPersistentStorage extends DistributedPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedChordPersistentStorage.class);

    private ConsistentHashRing hashRing;

    /**
     * Creates a new {@link DistributedChordPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkPersistentStorage the server to use for network communication
     */
    public DistributedChordPersistentStorage(NetworkPersistentStorage networkPersistentStorage) {
        super(networkPersistentStorage);
        hashRing = new TreeMapServerMetadata();
    }

    @Override
    protected KVMessage processResponseResiliently(String key, Callable<KVMessage> serverCallable,
                                                   KVMessage responseMessage) throws CommunicationClientException {
        KVMessage.StatusType responseStatus = responseMessage.getStatus();
        LOGGER.debug("Server indicated status '{}'", responseStatus);
        if (responseStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
            return handleServerNotResponsible(serverCallable, key);
        else if (responseStatus == KVMessage.StatusType.SERVER_STOPPED)
            return retryWithBackOff(serverCallable);
        else if (responseStatus == KVMessage.StatusType.SERVER_WRITE_LOCK)
            return retryWithBackOff(serverCallable);
        else {
            LOGGER.debug("Server indicated no status that requires additional action. Therefore, returning message.");
            return responseMessage;
        }
    }

    private KVMessage handleServerNotResponsible(Callable<KVMessage> serverCallable, String key) throws CommunicationClientException {
        LOGGER.debug("Requesting new metadata from server");
        updateHashRing();

        NetworkLocation responsibleNetLocation = hashRing.getWriteResponsibleNetworkLocation(key)
                .orElseThrow(() -> {
                    var exception = new CommunicationClientException("Could not find server responsible for data");
                    LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
                    return exception;
                });
        LOGGER.debug("Connecting to new {}", NetworkLocation.class.getSimpleName());
        connectAndReceive(
                responsibleNetLocation.getAddress(),
                responsibleNetLocation.getPort()
        );

        LOGGER.debug("Retrying with new server");
        try {
            final KVMessage newResponse = serverCallable.call();
            return handleSecondServerResponse(serverCallable, key, newResponse);
        } catch (Exception ex) {
            var exception = new CommunicationClientException(ex,
                    "The storage encountered an error. " + ex.getMessage());
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private void updateHashRing() throws CommunicationClientException {
        final KVMessage keyRangeRequest = new KVMessageImpl(KVMessage.StatusType.KEYRANGE);
        final CommunicationCallable getKeyRangeFunction = () -> sendAndReceive(keyRangeRequest);

        KVMessage keyRangeResponse = getKeyRangeFunction.call();
        final KVMessage.StatusType keyRangeStatus = keyRangeResponse.getStatus();

        if (keyRangeStatus == KVMessage.StatusType.SERVER_STOPPED) {
            retryWithBackOff(getKeyRangeFunction);
        } else if (keyRangeStatus != KVMessage.StatusType.KEYRANGE_SUCCESS) {
            var exception = new CommunicationClientException("Server did not respond with appropriate server metadata");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }

        try {
            hashRing = ConsistentHashRing.unpackMetadata(keyRangeResponse.getKey());
        } catch (IllegalArgumentException ex) {
            var exception = new CommunicationClientException(ex, "Could not unpack server metadata");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private KVMessage handleSecondServerResponse(Callable<KVMessage> serverCallable, String key,
                                                 KVMessage newResponse) throws CommunicationClientException {
        final KVMessage.StatusType newStatus = newResponse.getStatus();
        LOGGER.debug("Second server indicated status {}", newStatus);

        if (newStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            final var exception = new CommunicationClientException("Could not find responsible server");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        } else return processResponseResiliently(key, serverCallable, newResponse);
    }

    private interface CommunicationCallable extends Callable<KVMessage> {

        @Override
        KVMessage call() throws CommunicationClientException;

    }

}
