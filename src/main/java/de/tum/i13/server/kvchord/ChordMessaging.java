package de.tum.i13.server.kvchord;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ChordMessaging {
    private static final Logger LOGGER = LogManager.getLogger(ChordMessaging.class);

    private final Chord chordInstance;

    public ChordMessaging(Chord chordInstance) {
        this.chordInstance = chordInstance;
    }

    private static KVMessage connectSendAndReceive(NetworkLocation peer, KVMessage outgoingMessage,
            KVMessage.StatusType expectedStatus) {
        try (CommunicationClient communications = new CommunicationClient()) {
            communications.connect(peer.getAddress(), peer.getPort());
            String responseRaw = communications.receive();
            communications.send(outgoingMessage.packMessage());
            responseRaw = communications.receive();
            KVMessage response = KVMessage.unpackMessage(responseRaw);

            if (response == null) {
                LOGGER.error("Could not unpack response from {}: {}", peer, responseRaw);
                return null;
            }

            if (!response.getStatus().equals(expectedStatus)) {
                LOGGER.error("Message {} did not get expected response (expected {}, was {})", outgoingMessage,
                        expectedStatus, response.getStatus());
                return null;
            }

            return response;
        } catch (CommunicationClientException e) {
            LOGGER.atError()
                    .withThrowable(e)
                    .log("An error occured while sending {} to {}", outgoingMessage, peer);
            return null;
        }
    }

    public NetworkLocation findSuccessor(NetworkLocation peer, BigInteger key) {
        LOGGER.debug("Asking {} for successor of {} (findSuccessor)", peer, key.toString(16));
        
        if (peer.equals(this.chordInstance.getLocation())) {
            try {
                NetworkLocation result = this.chordInstance.findSuccessor(key);
                LOGGER.debug("Successor of {} is {}", key.toString(16), result);
                return result;
            } catch (ChordException e) {
                LOGGER.debug("Successor of {} is {}", key.toString(16), NetworkLocation.getNull());
                return NetworkLocation.getNull();
            }
        }

        KVMessage outgoingMessage = new KVMessageImpl(key.toString(16), KVMessage.StatusType.CHORD_FIND_SUCCESSOR);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_FIND_SUCESSSOR_RESPONSE);
        
        NetworkLocation result = response == null ? NetworkLocation.getNull() : NetworkLocation.extractNetworkLocation(response.getValue());
        LOGGER.debug("Successor of {} is {}", key.toString(16), result);

        return result;
    }

    public NetworkLocation closestPrecedingFinger(NetworkLocation peer, BigInteger key) {
        LOGGER.debug("Asking {} for closest preceding of {} (closestPrecedingFinger)", peer, key.toString(16));

        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.closestPrecedingFinger(key);
            LOGGER.debug("Closest preceeding of {} is {}", key, result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(key.toString(16),
                KVMessage.StatusType.CHORD_CLOSEST_PRECEDING_FINGER);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE);
        NetworkLocation result = response == null ? NetworkLocation.getNull() : NetworkLocation.extractNetworkLocation(response.getValue());
        LOGGER.debug("Closest preceeding of {} is {}", key, result);
        return result;
    }

    public NetworkLocation getPredecessor(NetworkLocation peer) throws ChordException {

        LOGGER.debug("Asking {} for it's predecessor (getPredecessor)", peer);

        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.getPredecessor();
            LOGGER.debug("Perceived predecessor of {} is {}", peer, result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(KVMessage.StatusType.CHORD_GET_PREDECESSOR);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_GET_PREDECESSOR_RESPONSE);

        if(response == null) {
            throw new ChordException("Could not get predecessor from peer");
        }

        NetworkLocation result = NetworkLocation.extractNetworkLocation(response.getKey());

        LOGGER.debug("Perceived predecessor of {} is {}", peer, result);

        return result;
    }

    public NetworkLocation getSuccessor(NetworkLocation peer) {

        LOGGER.debug("Asking {} for it's successor (getSuccessor)", peer);

        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.getSuccessor();
            LOGGER.debug("Successor of {} is {}", peer, result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl("1", KVMessage.StatusType.CHORD_GET_SUCCESSORS);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_GET_SUCCESSOR_RESPONSE);
        NetworkLocation result = response == null ? NetworkLocation.getNull() : NetworkLocation.extractNetworkLocation(response.getKey());

        LOGGER.debug("Successor of {} is {}", peer, result);

        return result;
    }

    public List<NetworkLocation> getSuccessors(NetworkLocation peer, int n) {

        LOGGER.debug("Asking {} for it's successors (getSuccessors)", peer);

        if (peer.equals(this.chordInstance.getLocation())) {
            List<NetworkLocation> result = this.chordInstance.getSuccessors(n);
            LOGGER.debug("Peer {} returned list with {} successors", peer, result.size());
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(Integer.toString(n), KVMessage.StatusType.CHORD_GET_SUCCESSORS);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_GET_SUCCESSOR_RESPONSE);


        if(response == null) {
            LOGGER.debug("Query peer for successors returned null");
            return new ArrayList<>();
        }

        String[] networkLocationsStr = response.getKey().split(",");

        List<NetworkLocation> result  = networkLocationsStr[0].equals("NO_SUCCS") ? new LinkedList<>() : Arrays
            .asList(networkLocationsStr)
            .stream()
            .map(NetworkLocation::extractNetworkLocation)
            .collect(Collectors.toList());
        
            LOGGER.debug("Peer {} returned list with {} successors", peer, result.size());

        return result;
    }

    public void notifyNode(NetworkLocation peer) {
        LOGGER.debug("Notifying {} (notify)", peer);
        KVMessage outgoingMessage = new KVMessageImpl(NetworkLocation.toPackedString(chordInstance.getLocation()), KVMessage.StatusType.CHORD_NOTIFY);

        // TODO: do something if it fails
        ChordMessaging.connectSendAndReceive(peer, outgoingMessage, KVMessage.StatusType.CHORD_NOTIFY_ACK);
    }

    public boolean isNodeAlive(NetworkLocation peer) {
        LOGGER.debug("Sending hearbeat to {}", peer);
        KVMessage outgoingMessage = new KVMessageImpl(KVMessage.StatusType.CHORD_HEARTBEAT);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage, KVMessage.StatusType.CHORD_HEARTBEAT_RESPONSE);

        return response != null;
    }

    @SuppressWarnings("java:S2189")
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            // Create the console object
            System.out.print("Enter command: ");
            // Read line
            String str = reader.readLine();

            try {
                final NetworkLocation peer = NetworkLocation.extractNetworkLocation(String.format("127.0.0.1:%s", str));
                KVMessage response = ChordMessaging.connectSendAndReceive(
                        peer,
                        new KVMessageImpl(StatusType.CHORD_GET_STATE_STR),
                        StatusType.CHORD_GET_STATE_STR_RESPONSE);
                System.out.println(response == null ? "COULD NOT FETCH RESPONSE" : response.getKey());
            } catch (NumberFormatException ex) {
                LOGGER.warn("Could not parse port number", ex);
            }

        }
    }
}
