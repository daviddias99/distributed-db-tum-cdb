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

import java.util.List;
import java.util.concurrent.Callable;

/**
 * A {@link WrappingPersistentStorage} that is aware of the distributed nature of the
 * {@link DistributedECSPersistentStorage}.
 * Uses a {@link ConsistentHashRing} to store server metadata.
 */
public class DistributedECSPersistentStorage extends DistributedPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedECSPersistentStorage.class);
    private ConsistentHashRing hashRing;

    /**
     * Creates a new {@link DistributedECSPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkPersistentStorage the server to use for network communication
     */
    public DistributedECSPersistentStorage(NetworkPersistentStorage networkPersistentStorage) {
        super(networkPersistentStorage);
        hashRing = new TreeMapServerMetadata();
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

    @Override
    protected List<NetworkLocation> getResponsibleNetworkLocations(String key, RequestType requestType,
                                                             KVMessage responseMessage) throws CommunicationClientException {
        LOGGER.debug("Requesting new metadata from server");
        updateHashRing();
        return switch (requestType) {
            case PUT -> hashRing.getWriteResponsibleNetworkLocation(key)
                    .map(List::of)
                    .orElseThrow(() -> {
                        var exception = new CommunicationClientException("Could not find server responsible for data");
                        LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
                        return exception;
                    });
            case GET -> hashRing.getReadResponsibleNetworkLocation(key);
        };
    }

    private interface CommunicationCallable extends Callable<KVMessage> {

        @Override
        KVMessage call() throws CommunicationClientException;

    }

}
