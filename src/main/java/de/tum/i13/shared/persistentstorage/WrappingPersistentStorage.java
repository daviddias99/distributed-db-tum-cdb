package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link PersistentStorage} that is connected remotely to a server via a
 * {@link NetworkMessageServer} and wraps
 * around it. This storage hides the network communication from the user.
 */
public class WrappingPersistentStorage implements NetworkPersistentStorage {

    public enum MessageMode {
        CLIENT,
        SERVER,
        SERVER_OWNER
    }

    private static final Logger LOGGER = LogManager.getLogger(WrappingPersistentStorage.class);
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";
    private static final String KEY_MAX_LENGTH_EXCEPTION_FORMAT = "Key '%s' exceeded maximum byte length of %s";
    private final NetworkMessageServer networkMessageServer;
    private MessageMode messageMode;

    /**
     * Creates a new {@link WrappingPersistentStorage} that wraps around the given
     * {@link NetworkMessageServer}
     *
     * @param networkMessageServer the server to use for network communication
     */
    public WrappingPersistentStorage(NetworkMessageServer networkMessageServer) {
        this(networkMessageServer, MessageMode.CLIENT);
    }

    /**
     * Creates a new {@link WrappingPersistentStorage} that wraps around the given
     * {@link NetworkMessageServer}
     *
     * @param networkMessageServer the server to use for network communication
     */
    public WrappingPersistentStorage(NetworkMessageServer networkMessageServer, MessageMode messageMode) {
        this.networkMessageServer = networkMessageServer;
        this.messageMode = messageMode;
    }

    @Override
    public KVMessage get(String key) throws GetException {
        LOGGER.info("Trying to get value of key '{}'", key);

        if (getByteLength(key) >= Constants.MAX_KEY_SIZE_BYTES) {
            throw new GetException(KEY_MAX_LENGTH_EXCEPTION_FORMAT, key,
                    Constants.MAX_KEY_SIZE_BYTES);
        }

        final KVMessageImpl getMessage = new KVMessageImpl(key, KVMessage.StatusType.GET);
        try {
            return sendAndReceive(getMessage);
        } catch (CommunicationClientException exception) {
            throw new GetException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        LOGGER.info("Trying to put key '{}' to value '{}'", key, value);

        // This code is duplicated from the get function due to different thrown
        // exception that cannot be handled
        // in a common method
        if (getByteLength(key) >= Constants.MAX_KEY_SIZE_BYTES) {
            throw new PutException(KEY_MAX_LENGTH_EXCEPTION_FORMAT, key,
                    Constants.MAX_KEY_SIZE_BYTES);
        }

        if (value != null && getByteLength(value) >= Constants.MAX_VALUE_SIZE_BYTES) {
            throw new PutException("Value '%s' exceeded maximum byte length of %s",
                    value, Constants.MAX_VALUE_SIZE_BYTES);
        }

        return value == null ? deleteKey(key) : putKey(key, value);
    }

    private int getByteLength(String string) {
        return string.getBytes(Constants.TELNET_ENCODING).length;
    }

    private KVMessage deleteKey(String key) throws PutException {
        LOGGER.debug("Trying to delete key '{}'", key);
        KVMessage.StatusType status = this.getDelMessageStatus();
        final KVMessage deleteMessage = new KVMessageImpl(key, status);
        return sendPutOrDeleteMessage(deleteMessage);
    }

    private KVMessage putKey(String key, String value) throws PutException {
        LOGGER.debug("Trying to put key '{}' to supplied value '{}'", key, value);
        KVMessage.StatusType status = this.getPutMessageStatus();
        final KVMessage putMessage = new KVMessageImpl(key, value, status);
        return sendPutOrDeleteMessage(putMessage);
    }

    private KVMessage sendPutOrDeleteMessage(KVMessage putOrDeleteMessage) throws PutException {
        try {
            return sendAndReceive(putOrDeleteMessage);
        } catch (CommunicationClientException exception) {
            throw new PutException(exception, EXCEPTION_FORMAT, exception.getMessage());
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

    // TODO: It should maybe be a different interface
    @Override
    public List<Pair<String>> getRange(String lowerBound, String upperBound) {
        return new LinkedList<>();
    }

    private KVMessage.StatusType getPutMessageStatus() {
        return switch (this.messageMode) {
            case CLIENT -> KVMessage.StatusType.PUT;        
            case SERVER -> KVMessage.StatusType.PUT_SERVER;
            case SERVER_OWNER -> KVMessage.StatusType.PUT_SERVER_OWNER;
        };
    }

    private KVMessage.StatusType getDelMessageStatus() {
        return switch (this.messageMode) {
            case CLIENT -> KVMessage.StatusType.DELETE;        
            case SERVER -> KVMessage.StatusType.DELETE_SERVER;
            case SERVER_OWNER -> KVMessage.StatusType.DELETE_SERVER;
        };
    }

    public void setMessageMode(MessageMode mode) {
        this.messageMode = mode;
    }
}
