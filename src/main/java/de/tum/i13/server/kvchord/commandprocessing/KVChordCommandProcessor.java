package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.CommandProcessor;

public class KVChordCommandProcessor implements CommandProcessor<KVMessage> {

  private Chord chord;

  public KVChordCommandProcessor(Chord chord) {
    this.chord = chord;
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      default -> null;
    };
  }
}
