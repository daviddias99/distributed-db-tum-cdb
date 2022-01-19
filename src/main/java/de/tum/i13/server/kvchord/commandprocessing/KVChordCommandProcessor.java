package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.kvchord.Chord;

import java.math.BigInteger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;

public class KVChordCommandProcessor implements CommandProcessor<KVMessage> {

  private Chord chord;

  public KVChordCommandProcessor(Chord chord) {
    this.chord = chord;
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      case CHORD_CLOSEST_PRECEDING_FINGER -> this.closestPreceding(command.getKey());
      case CHORD_FIND_SUCCESSOR -> this.findSuccessor(command.getKey());
      case CHORD_GET_PREDECESSOR -> this.getPredecessor();
      case CHORD_NOTIFY -> this.notifyChord(command.getKey());
      default -> null;
    };
  }

  private KVMessage closestPreceding(String key) {
    BigInteger keyNumber = new BigInteger(key, 16);
    NetworkLocation cpn = this.chord.closestPrecedingFinger(keyNumber);

    return new KVMessageImpl(key, cpn.toString(), StatusType.CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE);
  }
  private KVMessage findSuccessor(String key) {
    BigInteger keyNumber = new BigInteger(key, 16);
    NetworkLocation suc = this.chord.findSuccessor(keyNumber);

    return new KVMessageImpl(key, suc.toString(), StatusType.CHORD_FIND_SUCESSSOR_RESPONSE);
  }
  private KVMessage getPredecessor() {
    return new KVMessageImpl(this.chord.getPredecessor().toString(), StatusType.CHORD_GET_PREDECESSOR_RESPONSE);
  }
  private KVMessage notifyChord(String peerAddr) {
    NetworkLocation peer = NetworkLocation.extractNetworkLocation(peerAddr);
    this.chord.notifyNode(peer);
    return new KVMessageImpl(StatusType.CHORD_NOTIFY_ACK);
  }
}
