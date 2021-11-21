package de.tum.i13.client.net;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link PersistentStorage} that is connected remotely to a server via a {@link NetworkMessageServer}.
 * This storage hides the network communication from the user
 */
public class RemotePersistentStorage implements PersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(RemotePersistentStorage.class);
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";
    private final NetworkMessageServer networkMessageServer;

    /**
     * Creates a new remote persistent storage with the given network component
     *
     * @param networkMessageServer the server to use for network communication
     */
    public RemotePersistentStorage(NetworkMessageServer networkMessageServer) {
        this.networkMessageServer = networkMessageServer;
    }

    @Override
    public KVMessage get(String key) throws GetException {
        LOGGER.info("Trying to get value of key {}", key);

        final KVMessageImpl getMessage = new KVMessageImpl(key, KVMessage.StatusType.GET);
        try {
            return sendAndReceive(getMessage);
        } catch (ClientException exception) {
            LOGGER.error("Caught exception while getting. Wrapping the exception.", exception);
            throw new GetException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    @Override
    public KVMessage put(String key, String value) throws PutException {
        LOGGER.info("Trying to put key {} to value {}", key, value);

        final KVMessageImpl putMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);
        try {
            return sendAndReceive(putMessage);
        } catch (ClientException exception) {
            LOGGER.error("Caught exception while putting. Wrapping the exception.", exception);
            throw new PutException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    private KVMessage sendAndReceive(KVMessage message) throws ClientException {
        LOGGER.debug("Sending message to server: {}", message::packMessage);
        networkMessageServer.send((message.packMessage() + Constants.TERMINATING_STR).getBytes());
        return KVMessage.unpackMessage(receiveMessage());
    }

    /**
     * Called after sending a message to the server. Receives server's response in bytes, coverts it to String
     * in proper encoding and returns it.
     *
     * @return message received by the server
     * @throws ClientException if the message cannot be received
     */
    private String receiveMessage() throws ClientException {
        LOGGER.debug("Receiving message from server");
        byte[] response = networkMessageServer.receive();
        final String responseString = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
        LOGGER.debug("Received message from server: {}", responseString);
        return responseString;
    }

    public NetworkLocation getNetworkLocation() {
        return getNetworkMessageServer();
    }

    public NetworkMessageServer getNetworkMessageServer() {
        return networkMessageServer;
    }

}
