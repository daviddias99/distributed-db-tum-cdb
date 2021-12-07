package de.tum.i13.shared.persistentstorage;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.kv.KVMessage;
import de.tum.i13.shared.kv.KVMessageImpl;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link PersistentStorage} that is connected remotely to a server via a {@link NetworkMessageServer} and wraps
 * around it. This storage hides the network communication from the user.
 */
public class WrappingPersistentStorage implements NetworkPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(WrappingPersistentStorage.class);
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";
    private static final String KEY_MAX_LENGTH_EXCEPTION_FORMAT = "Key '%s' exceeded maximum byte length of %s";
    private final NetworkMessageServer networkMessageServer;
    private ConsistentHashRing hashRing;

    /**
     * Creates a new {@link WrappingPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkMessageServer the server to use for network communication
     */
    public WrappingPersistentStorage(NetworkMessageServer networkMessageServer) {
        this.networkMessageServer = networkMessageServer;
        this.hashRing = new TreeMapServerMetadata();
    }

    @Override
    public KVMessage get(String key) throws GetException {
        LOGGER.info("Trying to get value of key '{}'", key);

        if (getByteLength(key) >= Constants.MAX_KEY_SIZE_BYTES) {
            final GetException getException = new GetException(KEY_MAX_LENGTH_EXCEPTION_FORMAT, key,
                    Constants.MAX_KEY_SIZE_BYTES);
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
            throw getException;
        }

        final KVMessageImpl getMessage = new KVMessageImpl(key, KVMessage.StatusType.GET);
        try {
            return safeSendAndReceive(getMessage);
        } catch (CommunicationClientException exception) {
            LOGGER.error("Caught exception while getting. Wrapping the exception.", exception);
            throw new GetException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        LOGGER.info("Trying to put key '{}' to value '{}'", key, value);

        // This code is duplicated from the get function due to different thrown exception that cannot be handled
        // in a common method
        if (getByteLength(key) >= Constants.MAX_KEY_SIZE_BYTES) {
            final PutException putException = new PutException(KEY_MAX_LENGTH_EXCEPTION_FORMAT, key,
                    Constants.MAX_KEY_SIZE_BYTES);
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
            throw putException;
        }

        if (value != null && getByteLength(value) >= Constants.MAX_VALUE_SIZE_BYTES) {
            final PutException putException = new PutException("Value '%s' exceeded maximum byte length of %s",
                    value, Constants.MAX_VALUE_SIZE_BYTES);
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
            throw putException;
        }

        return value == null ? deleteKey(key) : putKey(key, value);
    }

    private int getByteLength(String string) {
        return string.getBytes(Constants.TELNET_ENCODING).length;
    }

    private KVMessage deleteKey(String key) throws PutException {
        LOGGER.debug("Trying to delete key '{}'", key);
        final KVMessage deleteMessage = new KVMessageImpl(key, KVMessage.StatusType.DELETE);
        return sendPutOrDeleteMessage(deleteMessage);
    }

    private KVMessage putKey(String key, String value) throws PutException {
        LOGGER.debug("Trying to put key '{}' to supplied value '{}'", key, value);
        final KVMessage putMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);
        return sendPutOrDeleteMessage(putMessage);
    }

    private KVMessage sendPutOrDeleteMessage(KVMessage putOrDeleteMessage) throws PutException {
        try {
            return safeSendAndReceive(putOrDeleteMessage);
        } catch (CommunicationClientException exception) {
            LOGGER.atError()
                    .withThrowable(exception)
                    .log("Caught exception while {}. Wrapping the exception.",
                            putOrDeleteMessage.getStatus() == KVMessage.StatusType.PUT ? "putting" : "deleting");
            throw new PutException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    private KVMessage safeSendAndReceive(KVMessage message) throws CommunicationClientException {
        KVMessage responseMessage = unsafeSendAndReceive(message);
        KVMessage.StatusType responseStatus = responseMessage.getStatus();

        if (responseStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            return handleServerNotResponsible(message);
        } else if (responseStatus == KVMessage.StatusType.SERVER_STOPPED) {
            // TODO Retry with backoff
            return null;
        } else if (responseStatus == KVMessage.StatusType.SERVER_WRITE_LOCK) {
            // TODO Retry with backoff
            return null;
        } else {
            return responseMessage;
        }
    }

    private KVMessage unsafeSendAndReceive(KVMessage message) throws CommunicationClientException {
        String packedMessage = message.packMessage();

        LOGGER.debug("Sending message to server: '{}'", packedMessage);
        networkMessageServer.send(packedMessage);

        LOGGER.debug("Receiving message from server");
        final String response = networkMessageServer.receive();
        LOGGER.debug("Received message from server: '{}'", response);

        try {
            return KVMessage.unpackMessage(response);
        } catch (IllegalArgumentException ex) {
            var exception = new CommunicationClientException(ex, "Could not unpack message received by the server");
            LOGGER.fatal(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private KVMessage handleServerNotResponsible(KVMessage message) throws CommunicationClientException {
        LOGGER.debug("Server indicated it is not responsible");

        processKeyRange(unsafeSendAndReceive(new KVMessageImpl(KVMessage.StatusType.KEYRANGE)));

        NetworkLocation responsibleNetLocation = hashRing.getResponsibleNetworkLocation(message.getKey())
                .orElseThrow(() -> {
                    var exception = new CommunicationClientException("Could not find server responsible for data");
                    LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
                    return exception;
                });
        LOGGER.debug("Connecting to new {}", NetworkLocation.class.getSimpleName());
        networkMessageServer.connectAndReceive(
                responsibleNetLocation.getAddress(),
                responsibleNetLocation.getPort()
        );

        return unsafeSendAndReceive(message);
    }

    private void processKeyRange(KVMessage keyRangeMessage) throws CommunicationClientException {
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

    @Override
    public void send(String message) throws CommunicationClientException {
        networkMessageServer.send(message);
    }

    @Override
    public String receive() throws CommunicationClientException {
        return networkMessageServer.receive();
    }

    @Override
    public void connect(String address, int port) throws CommunicationClientException {
        networkMessageServer.connect(address, port);
    }

    @Override
    public void disconnect() throws CommunicationClientException {
        networkMessageServer.disconnect();
    }

    @Override
    public boolean isConnected() {
        return networkMessageServer.isConnected();
    }

    @Override
    public String getAddress() {
        return networkMessageServer.getAddress();
    }

    @Override
    public int getPort() {
        return networkMessageServer.getPort();
    }

}
