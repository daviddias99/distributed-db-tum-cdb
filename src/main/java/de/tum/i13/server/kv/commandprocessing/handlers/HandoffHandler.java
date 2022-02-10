package de.tum.i13.server.kv.commandprocessing.handlers;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Handler that manages chunk handoff to other servers
 */
public class HandoffHandler implements Runnable {


    private final PersistentStorage storage;
    private final ServerCommunicator ecs;
    private final NetworkLocation peer;
    private final String lowerBound;
    private final String upperBound;
    private final boolean async;
    private final boolean isShutdown;
    private final List<String> nodesToDelete;
    private final ECSServerState state;
    private final HashingAlgorithm hashingAlgorithm;

    /**
     * Create a new handoff handler
     *
     * @param peer       target server
     * @param ecs        ECS communications interface
     * @param lowerBound lower bound for the keys of the transfered elements
     * @param upperBound upper bound for the keys of the transfered elements
     * @param storage    current server storage
     */
    public HandoffHandler(NetworkLocation peer, ServerCommunicator ecs, String lowerBound, String upperBound,
                          PersistentStorage storage, boolean async, ECSServerState state,
                          HashingAlgorithm hashingAlgorithm) {
        this.storage = storage;
        this.peer = peer;
        this.ecs = ecs;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.async = async;
        this.state = state;
        this.nodesToDelete = state.getDeleteQueue();
        this.isShutdown = state.isShutdown();
        this.hashingAlgorithm = hashingAlgorithm;
    }

    @Override
    public void run() {
        final Logger LOGGER = LogManager.getLogger(HandoffHandler.class);

        NetworkPersistentStorage netPeerStorage = new WrappingPersistentStorage(new CommunicationClient(),
                WrappingPersistentStorage.MessageMode.SERVER);

        try {
            LOGGER.info("Trying to connect to peer {} for handhoff", peer);
            netPeerStorage.connectAndReceive(peer.getAddress(), peer.getPort());
        } catch (CommunicationClientException e) {
            LOGGER.error("Could not connect to peer {} for handoff.", peer, e);
            return;
        }

        List<Pair<String>> itemsToSend = new LinkedList<>();

        // Fetch items from storage
        try {
            LOGGER.info("Fetching range from database");
            itemsToSend = this.getRange(lowerBound, upperBound);
        } catch (GetException e) {
            LOGGER.error("Error while getting key range during handoff.", e);
        }

        // Send items to peer
        for (Pair<String> item : itemsToSend) {
            try {
                LOGGER.info("Sending item with key {} to peer {}.", item.key, peer);
                KVMessage response = netPeerStorage.put(item.key, item.value);

                if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                    LOGGER.info("Sent item with key {} to peer {}.", item.key, peer);
                    nodesToDelete.add(item.key);
                } else {
                    LOGGER.error("Failed to send item with key {} to peer {}.", item.key, peer);
                }
            } catch (PutException e) {
                LOGGER.error("Could not send item with key {} to peer {}.", item.key, peer, e);
            }
        }

        // Delete items after sending (only sucessful ones)
        if (this.isShutdown) {
            this.state.executeStoredDeletes(storage);
            this.state.deleteReplicatedRanges();
        }

        if (this.async) {
            try {
                LOGGER.info("Finished sync. handoff.");
                ecs.confirmHandoff();
            } catch (CommunicationClientException ex) {
                LOGGER.error("Could not notify handoff success", ex);
            }
        }
    }

    private List<Pair<String>> getRange(String lowerBound, String upperBound) throws GetException {

        int hashSize = this.hashingAlgorithm.getHashSizeBits() / 4;
        String paddedLower = HashingAlgorithm.padLeftZeros(lowerBound, hashSize);
        String paddedUpper = HashingAlgorithm.padLeftZeros(upperBound, hashSize);

        if (paddedLower.compareTo(paddedUpper) <= 0) {
            return this.storage.getRange(paddedLower, paddedUpper);
        }

        List<Pair<String>> result = new LinkedList<>();
        result.addAll(this.storage.getRange("0".repeat(hashSize), paddedUpper));
        result.addAll(this.storage.getRange(paddedLower, "f".repeat(hashSize)));

        return result;
    }

}
