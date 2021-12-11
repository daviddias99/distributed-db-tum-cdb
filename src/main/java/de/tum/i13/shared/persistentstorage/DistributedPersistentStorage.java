package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkMessageServer;
import io.github.resilience4j.retry.MaxRetriesExceededException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

/**
 * A {@link WrappingPersistentStorage} that is aware of the distributed nature of the
 * {@link DistributedPersistentStorage}.
 * Uses a {@link ConsistentHashRing} to store server metadata.
 */
public class DistributedPersistentStorage implements NetworkPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedPersistentStorage.class);
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";
    private static final RetryConfig retryConfig = RetryConfig.<KVMessage>custom()
            .maxAttempts(Constants.MAX_REQUEST_RETRIES)
            .intervalFunction(ofExponentialRandomBackoff(
                    Constants.EXP_BACKOFF_INIT_INTERVAL,
                    Constants.EXP_BACKOFF_MULTIPLIER,
                    Constants.EXP_BACKOFF_RAND_FACTOR
            ))
            .retryOnResult(message -> {
                var status = message.getStatus();
                LOGGER.debug("Server indicated status '{}'", status);
                return status == KVMessage.StatusType.SERVER_WRITE_LOCK
                        || status == KVMessage.StatusType.SERVER_STOPPED;
            })
            .failAfterMaxAttempts(true)
            .retryOnException(ex -> false)
            .build();
    private static final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
    private final NetworkPersistentStorage persistentStorage;

    private ConsistentHashRing hashRing;

    /**
     * Creates a new {@link DistributedPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkPersistentStorage the server to use for network communication
     */
    public DistributedPersistentStorage(NetworkPersistentStorage networkPersistentStorage) {
        this.persistentStorage = networkPersistentStorage;
        hashRing = new TreeMapServerMetadata();
    }

    /**
     * {@inheritDoc} This function is aware of the distributed nature of the {@link PersistentStorage}.
     */
    @Override
    public KVMessage get(String key) throws GetException {
        LOGGER.info("Trying to get value of key '{}'", key);

        final KVMessage responseMessage = persistentStorage.get(key);
        final Callable<KVMessage> serverCallable = () -> persistentStorage.get(key);

        try {
            return callServerResiliently(key, serverCallable, responseMessage);
        } catch (CommunicationClientException exception) {
            final GetException getException = new GetException(exception, EXCEPTION_FORMAT, exception.getMessage());
            LOGGER.error("Caught exception while getting. Wrapping the exception.", getException);
            throw getException;
        }
    }

    /**
     * {@inheritDoc} This function is aware of the distributed nature of the {@link PersistentStorage}.
     */
    @Override
    public KVMessage put(String key, String value) throws PutException {
        LOGGER.info("Trying to put key '{}' value '{}'", key, value);

        final KVMessage responseMessage = persistentStorage.put(key, value);
        final Callable<KVMessage> serverCallable = () -> persistentStorage.put(key, value);

        try {
            return callServerResiliently(key, serverCallable, responseMessage);
        } catch (CommunicationClientException exception) {
            final PutException putException = new PutException(exception, EXCEPTION_FORMAT, exception.getMessage());
            LOGGER.error("Caught exception while getting. Wrapping the exception.", putException);
            throw putException;
        }
    }

    @Override
    public List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {
        return persistentStorage.getRange(lowerBound, upperBound);
    }

    private KVMessage callServerResiliently(String key, Callable<KVMessage> serverCallable,
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

    private KVMessage retryWithBackOff(Callable<KVMessage> serverCallable) throws CommunicationClientException {
        LOGGER.debug("Retrying using backoff with jitter");
        Retry retry = retryRegistry.retry("messageSending");
        try {
            return retry.executeCallable((() -> {
                LOGGER.debug("Retried to send message");
                return serverCallable.call();
            }));
        } catch (MaxRetriesExceededException ex) {
            var exception = new CommunicationClientException(ex, "Reached maximum number of retries while " +
                    "communicating with server");
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        } catch (Exception ex) {
            var exception = new CommunicationClientException(ex, "The storage encountered an error" + ex.getMessage());
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private KVMessage handleServerNotResponsible(Callable<KVMessage> serverCallable, String key) throws CommunicationClientException {
        LOGGER.debug("Requesting new metadata from server");
        updateHashRing();

        NetworkLocation responsibleNetLocation = hashRing.getResponsibleNetworkLocation(key)
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
        } else return callServerResiliently(key, serverCallable, newResponse);
    }

    @Override
    public void send(String message) throws CommunicationClientException {
        persistentStorage.send(message);
    }

    @Override
    public String receive() throws CommunicationClientException {
        return persistentStorage.receive();
    }

    @Override
    public void connect(String address, int port) throws CommunicationClientException {
        persistentStorage.connect(address, port);
    }

    @Override
    public void disconnect() throws CommunicationClientException {
        persistentStorage.disconnect();
    }

    @Override
    public boolean isConnected() {
        return persistentStorage.isConnected();
    }

    @Override
    public String getAddress() {
        return persistentStorage.getAddress();
    }

    @Override
    public int getPort() {
        return persistentStorage.getPort();
    }

    @Override
    public KVMessage sendAndReceive(KVMessage message) throws CommunicationClientException {
        return persistentStorage.sendAndReceive(message);
    }

    @Override
    public String connectAndReceive(String address, int port) throws CommunicationClientException {
        return persistentStorage.connectAndReceive(address, port);
    }

    private interface CommunicationCallable extends Callable<KVMessage> {

        @Override
        KVMessage call() throws CommunicationClientException;

    }

}
