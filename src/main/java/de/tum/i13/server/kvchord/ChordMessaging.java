package de.tum.i13.server.kvchord;

import java.io.Console;
import java.math.BigInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;

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
        LOGGER.debug("Asking {} for successor of {} (findSuccessor)", peer, key);
        
        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.findSuccessor(key);
            LOGGER.debug("Successor of {} is {}", key.toString(16), result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(key.toString(16), KVMessage.StatusType.CHORD_FIND_SUCCESSOR);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_FIND_SUCESSSOR_RESPONSE);
        
        NetworkLocation result = response == null ? null : NetworkLocation.extractNetworkLocation(response.getValue());
        LOGGER.debug("Successor of {} is {}", key.toString(16), result);

        return result;
    }

    public NetworkLocation closestPrecedingFinger(NetworkLocation peer, BigInteger key) {
        LOGGER.debug("Asking {} for closest preceding of {} (closestPrecedingFinger)", peer, key);

        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.closestPrecedingFinger(key);
            LOGGER.debug("Closest preceeding of {} is {}", key, result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(key.toString(16),
                KVMessage.StatusType.CHORD_CLOSEST_PRECEDING_FINGER);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE);
        NetworkLocation result = response == null ? null : NetworkLocation.extractNetworkLocation(response.getValue());
        LOGGER.debug("Closest preceeding of {} is {}", key, result);
        return result;
    }

    public NetworkLocation getPredecessor(NetworkLocation peer) {

        LOGGER.debug("Asking {} for it's predecessor (getPredecessor)", peer);

        if (peer.equals(this.chordInstance.getLocation())) {
            NetworkLocation result = this.chordInstance.getPredecessor();
            LOGGER.debug("Perceived predecessor of {} is {}", peer, result);
            return result;
        }

        KVMessage outgoingMessage = new KVMessageImpl(KVMessage.StatusType.CHORD_GET_PREDECESSOR);
        KVMessage response = ChordMessaging.connectSendAndReceive(peer, outgoingMessage,
                KVMessage.StatusType.CHORD_GET_PREDECESSOR_RESPONSE);
        NetworkLocation result = response == null ? null : NetworkLocation.extractNetworkLocation(response.getKey());

        LOGGER.debug("Perceived predecessor of {} is {}", peer, result);

        return result;
    }

    public void notifyNode(NetworkLocation peer) {
        LOGGER.debug("Notifying {} (notify)", peer);
        KVMessage outgoingMessage = new KVMessageImpl(NetworkLocation.toPackedString(chordInstance.getLocation()), KVMessage.StatusType.CHORD_NOTIFY);

        // TODO: do something if it fails
        ChordMessaging.connectSendAndReceive(peer, outgoingMessage, KVMessage.StatusType.CHORD_NOTIFY_ACK);
    }

    public static void main(String[] args) {

        while (true) {
            // Create the console object
            Console cnsl = System.console();

            if (cnsl == null) {
                System.out.println(
                        "No console available");
                return;
            }

            // Read line
            String str = cnsl.readLine(
                    "Enter command : ");

            KVMessage response = ChordMessaging.connectSendAndReceive(
                NetworkLocation.extractNetworkLocation(String.format("127.0.0.1:%s", str)),
                new KVMessageImpl(StatusType.CHORD_GET_STATE_STR),
                StatusType.CHORD_GET_STATE_STR_RESPONSE);

            System.out.println(response.getKey());
        }
    }
}
