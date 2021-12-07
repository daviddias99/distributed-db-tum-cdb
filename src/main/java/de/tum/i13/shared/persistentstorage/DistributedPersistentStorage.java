package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkMessageServer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

/**
 * A {@link WrappingPersistentStorage} that is aware of the distributed nature of the persistent storage.
 * Uses a {@link ConsistentHashRing} to store server metadata.
 */
public class DistributedPersistentStorage extends WrappingPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedPersistentStorage.class);
    private static final RetryConfig retryConfig = RetryConfig.<KVMessage>custom()
            .maxAttempts(Constants.MAX_REQUEST_RETRIES)
            .intervalFunction(ofExponentialRandomBackoff(
                    Constants.EXP_BACKOFF_INIT_INTERVAL,
                    Constants.EXP_BACKOFF_MULTIPLIER,
                    Constants.EXP_BACKOFF_RAND_FACTOR
            ))
            .retryOnResult(message -> {
                var status = message.getStatus();
                return status == KVMessage.StatusType.SERVER_WRITE_LOCK
                        || status == KVMessage.StatusType.SERVER_STOPPED;
            })
            .retryOnException(ex -> false)
            .build();
    private static final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);

    private ConsistentHashRing hashRing;

    /**
     * Creates a new {@link DistributedPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkMessageServer the server to use for network communication
     */
    public DistributedPersistentStorage(NetworkMessageServer networkMessageServer) {
        super(networkMessageServer);
        hashRing = new TreeMapServerMetadata();
    }

    @Override
    protected KVMessage sendAndReceive(KVMessage message) throws CommunicationClientException {
        LOGGER.info("Sending message to server: '{}'", message);
        KVMessage responseMessage = super.sendAndReceive(message);
        KVMessage.StatusType responseStatus = responseMessage.getStatus();

        LOGGER.debug("Server indicated status '{}'", responseStatus);
        if (responseStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
            return handleServerNotResponsible(message);
        else if (responseStatus == KVMessage.StatusType.SERVER_STOPPED)
            return retryWithBackOff(message);
        else if (responseStatus == KVMessage.StatusType.SERVER_WRITE_LOCK)
            return retryWithBackOff(message);
        else {
            LOGGER.debug("Server indicated no status that requires additional action. Therefore, returning message.");
            return responseMessage;
        }
    }

    private KVMessage retryWithBackOff(KVMessage message) throws CommunicationClientException {
        LOGGER.debug("Retrying using backoff with jitter");
        Retry retry = retryRegistry.retry("messageSending");
        try {
            return retry.executeCallable(() -> super.sendAndReceive(message));
        } catch (Exception ex) {
            var exception = new CommunicationClientException(ex, "Reached maximum number of retries while " +
                    "communicating with server");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private KVMessage handleServerNotResponsible(KVMessage message) throws CommunicationClientException {
        LOGGER.debug("Requesting new metadata from server");
        processKeyRangeResponse(super.sendAndReceive(new KVMessageImpl(KVMessage.StatusType.KEYRANGE)));

        NetworkLocation responsibleNetLocation = hashRing.getResponsibleNetworkLocation(message.getKey())
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
        return super.sendAndReceive(message);
    }

    private void processKeyRangeResponse(KVMessage keyRangeMessage) throws CommunicationClientException {
        if (keyRangeMessage.getStatus() != KVMessage.StatusType.KEYRANGE_SUCCESS) {
            var exception = new CommunicationClientException("Server did not respond with appropriate server metadata");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }

        try {
            hashRing = ConsistentHashRing.unpackMetadata(keyRangeMessage.getKey());
        } catch (IllegalArgumentException ex) {
            var exception = new CommunicationClientException(ex, "Could not unpack server metadata");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

}
