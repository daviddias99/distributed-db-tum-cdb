package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import io.github.resilience4j.retry.MaxRetriesExceededException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

public abstract class DistributedPersistentStorage implements NetworkPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedPersistentStorage.class);
    private static final Random RANDOM = new Random();

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
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";
    protected final NetworkPersistentStorage persistentStorage;

    protected DistributedPersistentStorage(NetworkPersistentStorage networkPersistentStorage) {
        this.persistentStorage = networkPersistentStorage;
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
            return processResponseResiliently(key, RequestType.GET, serverCallable, responseMessage);
        } catch (CommunicationClientException exception) {
            throw new GetException(exception,
                    EXCEPTION_FORMAT, exception.getMessage());
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
            return processResponseResiliently(key, RequestType.PUT, serverCallable, responseMessage);
        } catch (CommunicationClientException exception) {
            throw new PutException(exception,
                    EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    @Override
    public List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {
        return persistentStorage.getRange(lowerBound, upperBound);
    }

    private KVMessage processResponseResiliently(String key, RequestType requestType,
                                                 Callable<KVMessage> serverCallable,
                                                 KVMessage responseMessage) throws CommunicationClientException {
        KVMessage.StatusType responseStatus = responseMessage.getStatus();
        LOGGER.debug("Server indicated status '{}'", responseStatus);
        return switch (responseStatus) {
            case SERVER_NOT_RESPONSIBLE -> handleServerNotResponsible(serverCallable, key, requestType,
                    responseMessage);
            case SERVER_STOPPED, SERVER_WRITE_LOCK -> retryWithBackOff(serverCallable);
            default -> {
                LOGGER.debug("Server indicated no status that requires additional action. Therefore, returning " +
                        "message.");
                yield responseMessage;
            }
        };
    }

    protected KVMessage retryWithBackOff(Callable<KVMessage> serverCallable) throws CommunicationClientException {
        LOGGER.debug("Retrying using backoff with jitter");
        Retry retry = retryRegistry.retry("messageSending");
        try {
            return retry.executeCallable((() -> {
                LOGGER.debug("Retried to send message");
                return serverCallable.call();
            }));
        } catch (MaxRetriesExceededException ex) {
            throw new CommunicationClientException(
                    ex,
                    "Reached maximum number of retries while communicating with server"
            );
        } catch (Exception ex) {
            throw new CommunicationClientException(ex, "The storage encountered an error" + ex.getMessage());
        }
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

    private KVMessage handleServerNotResponsible(Callable<KVMessage> serverCallable, String key,
                                                 RequestType requestType,
                                                 KVMessage responseMessage) throws CommunicationClientException {
        LOGGER.debug("Handling not responsible message from server");
        final List<NetworkLocation> responsibleNetworkLocations = getResponsibleNetworkLocations(key, requestType,
                responseMessage);
        final NetworkLocation responsibleNetLocation = getRandomNetworkLocation(responsibleNetworkLocations);

        LOGGER.debug("Connecting to new {}", NetworkLocation.class.getSimpleName());
        connectAndReceive(
                responsibleNetLocation.getAddress(),
                responsibleNetLocation.getPort()
        );

        LOGGER.debug("Retrying with new server");
        try {
            final KVMessage newResponse = serverCallable.call();
            return handleSecondServerResponse(serverCallable, key, requestType, newResponse);
        } catch (Exception ex) {
            throw new CommunicationClientException(ex, "The storage encountered an error. " + ex.getMessage());
        }
    }

    private NetworkLocation getRandomNetworkLocation(List<NetworkLocation> networkLocations) throws CommunicationClientException {
        if (networkLocations.isEmpty()) {
            throw new CommunicationClientException("Could not find server responsible for data");
        }
        if (networkLocations.size() == 1) {
            return networkLocations.get(0);
        }
        return networkLocations.get(RANDOM.nextInt(networkLocations.size()));
    }

    protected abstract List<NetworkLocation> getResponsibleNetworkLocations(String key, RequestType requestType,
                                                                            KVMessage responseMessage) throws CommunicationClientException;

    private KVMessage handleSecondServerResponse(Callable<KVMessage> serverCallable, String key,
                                                 RequestType requestType,
                                                 KVMessage newResponse) throws CommunicationClientException {
        final KVMessage.StatusType newStatus = newResponse.getStatus();
        LOGGER.debug("Second server indicated status {}", newStatus);

        if (newStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            throw new CommunicationClientException("Could not find responsible server");
        } else return processResponseResiliently(key, requestType, serverCallable, newResponse);
    }

    protected enum RequestType {
        PUT,
        GET
    }


}
