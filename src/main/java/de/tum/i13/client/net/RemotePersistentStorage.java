package de.tum.i13.client.net;

import de.tum.i13.server.kv.GetException;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemotePersistentStorage implements PersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(RemotePersistentStorage.class);

    private final NetworkMessageServer networkConnection;
    private static final String EXCEPTION_FORMAT = "Communication client threw exception: %s";

    public RemotePersistentStorage(NetworkMessageServer networkConnection) {
        this.networkConnection = networkConnection;
    }

    // TODO Add logging
    @Override
    public KVMessage get(String key) throws GetException {
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
        final KVMessageImpl putMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);
        try {
            return sendAndReceive(putMessage);
        } catch (ClientException exception) {
            LOGGER.error("Caught exception while putting. Wrapping the exception.", exception);
            throw new PutException(exception, EXCEPTION_FORMAT, exception.getMessage());
        }
    }

    private KVMessage sendAndReceive(KVMessage message) throws ClientException {
        LOGGER.debug("Sending message to server: {}", message.packMessage());
        networkConnection.send((message.packMessage() + Constants.TERMINATING_STR).getBytes());
        return KVMessage.unpackMessage(receiveMessage());
    }

    /**
     * Called after sending a message to the server. Receives server's response in bytes, coverts it to String
     * in proper encoding and returns it.
     *
     * @throws ClientException if the message cannot be received
     * @return message received by the server
     */
    private String receiveMessage() throws ClientException {
        LOGGER.debug("Receiving message from server.");
        byte[] response = networkConnection.receive();
        return new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
    }

    public NetworkLocation getNetworkLocation() {
        return networkConnection;
    }

}
