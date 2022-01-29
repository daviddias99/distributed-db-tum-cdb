package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.ChordException;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;

import java.math.BigInteger;
import java.util.stream.Collectors;

public class KVChordCommandProcessor implements CommandProcessor<KVMessage> {

    private final Chord chord;

    public KVChordCommandProcessor(Chord chord) {
        this.chord = chord;
    }

    @Override
    public KVMessage process(KVMessage command) {
        return switch (command.getStatus()) {
            case CHORD_CLOSEST_PRECEDING_FINGER -> this.closestPreceding(command.getKey());
            case CHORD_FIND_SUCCESSOR -> this.findSuccessor(command.getKey());
            case CHORD_GET_PREDECESSOR -> this.getPredecessor();
            case CHORD_GET_SUCCESSORS -> this.getSuccessors(Integer.parseInt(command.getKey()));
            case CHORD_NOTIFY -> this.notifyChord(command.getKey());
            case CHORD_GET_STATE_STR -> this.getState();
            case CHORD_HEARTBEAT -> new KVMessageImpl(StatusType.CHORD_HEARTBEAT_RESPONSE);
            default -> null;
        };
    }

    private KVMessage closestPreceding(String key) {
        BigInteger keyNumber = new BigInteger(key, 16);
        NetworkLocation cpn = this.chord.closestPrecedingFinger(keyNumber);

        return new KVMessageImpl(key, NetworkLocation.toPackedString(cpn),
                StatusType.CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE);
    }

    private KVMessage findSuccessor(String key) {
        BigInteger keyNumber = new BigInteger(key, 16);

        try {
            NetworkLocation suc = this.chord.findSuccessor(keyNumber);
            return new KVMessageImpl(key, NetworkLocation.toPackedString(suc),
                    StatusType.CHORD_FIND_SUCESSSOR_RESPONSE);
        } catch (ChordException e) {
            return new KVMessageImpl(StatusType.ERROR);
        }
    }

    private KVMessage getPredecessor() {
        return new KVMessageImpl(NetworkLocation.toPackedString(this.chord.getPredecessor()),
                StatusType.CHORD_GET_PREDECESSOR_RESPONSE);
    }

    private KVMessage getSuccessors(int successorCount) {
        String packedSuccs = this.chord.getSuccessors(successorCount).stream()
                .map(NetworkLocation::toPackedString)
                .collect(Collectors.joining(","));

        return new KVMessageImpl(packedSuccs.isEmpty() ? "NO_SUCCS" : packedSuccs, StatusType.CHORD_GET_SUCCESSOR_RESPONSE);
    }

    private KVMessage notifyChord(String peerAddr) {
        NetworkLocation peer = NetworkLocation.extractNetworkLocation(peerAddr);
        this.chord.notifyNode(peer);
        return new KVMessageImpl(StatusType.CHORD_NOTIFY_ACK);
    }

    private KVMessage getState() {
        String state = this.chord.getStateStr();
        return new KVMessageImpl(state, StatusType.CHORD_GET_STATE_STR_RESPONSE);
    }

}
