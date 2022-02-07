package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link PersistentStorage} that is also a {@link NetworkMessageServer}.
 */
public interface NetworkPersistentStorage extends PersistentStorage, NetworkMessageServer {

    /**
     * Convenience function that sends a message to server and receives the answer from the server.
     *
     * @param message the message to send
     * @return the message received from the server
     * @throws CommunicationClientException if the server message could not be unpacked, or something went wrong in
     * sending ore receiving the message
     */
    default KVMessage sendAndReceive(KVMessage message) throws CommunicationClientException {
        final Logger logger = LogManager.getLogger(NetworkPersistentStorage.class);
        String packedMessage = message.toString();

        logger.debug("Sending message to server: '{}'", packedMessage);
        send(packedMessage);

        logger.debug("Receiving message from server");
        final String response = receive();
        logger.debug("Received message from server: '{}'", response);

        try {
            return KVMessage.unpackMessage(response);
        } catch (IllegalArgumentException ex) {
            throw new CommunicationClientException(ex, "Could not unpack message received by the server");
        }
    }

}
