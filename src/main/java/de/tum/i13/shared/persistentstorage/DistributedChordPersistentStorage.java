package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * A {@link WrappingPersistentStorage} that is aware of the distributed nature of the
 * {@link DistributedChordPersistentStorage}.
 * Uses a {@link ConsistentHashRing} to store server metadata.
 */
public class DistributedChordPersistentStorage extends DistributedPersistentStorage {

    private static final Logger LOGGER = LogManager.getLogger(DistributedChordPersistentStorage.class);

    /**
     * Creates a new {@link DistributedChordPersistentStorage} that wraps around the given {@link NetworkMessageServer}
     *
     * @param networkPersistentStorage the server to use for network communication
     */
    public DistributedChordPersistentStorage(NetworkPersistentStorage networkPersistentStorage) {
        super(networkPersistentStorage);
    }

    @Override
    protected NetworkLocation getResponsibleNetworkLocation(String key, RequestType requestType, KVMessage responseMessage) throws CommunicationClientException {
        LOGGER.trace("Getting responsible network location for based on message '{}'", responseMessage);
        return Arrays.stream(responseMessage.getKey().split(","))
                .map(NetworkLocation::extractNetworkLocation)
                .findAny()
                .orElseThrow(
                        () -> new CommunicationClientException("Could not find server responsible for data from " +
                                "message {}", responseMessage)
                );
    }

}
