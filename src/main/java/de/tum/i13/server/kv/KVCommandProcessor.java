package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {

    private ServerState serverState;
    private PersistentStorage kvStore;
    private static final Logger LOGGER = LogManager.getLogger(KVCommandProcessor.class);

    public KVCommandProcessor(PersistentStorage storage, ServerState serverState) {
        this.kvStore = storage;
        this.serverState = serverState;
    }

    @Override
    public String process(String command, PeerType peerType) {
        if(this.serverState.isStopped() && !peerType.canBypassStop()) {
            return new KVMessageImpl(StatusType.SERVER_STOPPED).toString();
        }

        String[] tokens = KVMessage.extractTokens(command);
        KVMessage message = switch (tokens[0]) {
            case "get" -> this.get(tokens[1]);
            case "put" -> this.put(tokens[1], tokens[2]);
            case "delete" -> this.delete(tokens[1]);
            default -> new KVMessageImpl(StatusType.UNDEFINED);
        };

        return message.toString();
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: {}", remoteAddress);
        return "Connection to KVServer established: " + address.toString();
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        LOGGER.info("connection closed: {}", remoteAddress);
    }

    /**
     * Method to search for a key in the persistent storage.
     *
     * @param key the key to search
     * @return a KVMessage with the status of the query
     */
    private KVMessage get(String key) {
        try {
            LOGGER.info("Trying to read key: {}", key);
            return kvStore.get(key);
        } catch (GetException e) {
            LOGGER.error(e);
            return new KVMessageImpl(key, StatusType.GET_ERROR);
        }
    }

    /**
     * Helper method to store a new KV pair in the database
     *
     * @param key   the key of the new pair
     * @param value the new value
     * @return a KVMessage with the status of the put operation
     */
    private KVMessage put(String key, String value) {
        try {
            LOGGER.info("Trying to put key: {} and value: {}", key, value);
            return kvStore.put(key, value);
        } catch (PutException e) {
            LOGGER.error(e);
            return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
        }
    }

    private KVMessage delete(String key) {
        try {
            LOGGER.info("Trying to delete key: {}", key);
            return kvStore.put(key, null);
        } catch (PutException e) {
            LOGGER.error(e);
            return new KVMessageImpl(key, StatusType.DELETE_ERROR);
        }
    }

}
