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
        if (this.serverState.isStopped() && !peerType.canBypassStop()) {
            return new KVMessageImpl(StatusType.SERVER_STOPPED).toString();
        }

        KVMessage incomingMessage = KVMessage.unpackMessage(command);
        KVMessage response = null;

        if (peerType == PeerType.ECS) {
            response = processEcs(incomingMessage);
        } else if (peerType == PeerType.CLIENT) {
            response = processClient(incomingMessage);
        } else if (peerType == PeerType.SERVER) {
            response = processServer(incomingMessage);
        } 
        
        if(response == null || response.getStatus() == StatusType.UNDEFINED){
            response = processCommon(incomingMessage);
        }

        return response.toString();
    }

    // TODO: can possibly be each it's own class
    private KVMessage processEcs(KVMessage incomingMessage) {
        return switch (incomingMessage.getStatus()) {
            case HEART_BEAT -> new KVMessageImpl(StatusType.HEART_BEAT);
            default -> new KVMessageImpl(StatusType.UNDEFINED);
        };
    }

    private KVMessage processClient(KVMessage incomingMessage) {
        return switch (incomingMessage.getStatus()) {
            case GET -> this.get(incomingMessage.getValue());
            default -> new KVMessageImpl(StatusType.UNDEFINED);
        };
    }

    private KVMessage processServer(KVMessage incomingMessage) {
        return switch (incomingMessage.getStatus()) {
            default -> new KVMessageImpl(StatusType.UNDEFINED);
        };
    }

    private KVMessage processCommon(KVMessage incomingMessage) {
        return switch (incomingMessage.getStatus()) {
            case PUT -> this.put(incomingMessage.getKey(), incomingMessage.getValue());
            case DELETE -> this.delete(incomingMessage.getKey());
            default -> new KVMessageImpl(StatusType.UNDEFINED);
        };
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
