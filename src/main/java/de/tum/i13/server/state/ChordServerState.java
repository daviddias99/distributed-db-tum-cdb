package de.tum.i13.server.state;

import de.tum.i13.server.kvchord.Chord;

public class ChordServerState extends AbstractServerState implements ServerState {

    private final Chord chord;

    public ChordServerState(Chord chord) {
        super(State.ACTIVE);
        this.chord = chord;
    }

    @Override
    public boolean isWriteResponsible(String key) {
        return chord.isWriteResponsible(key);
    }

    @Override
    public boolean isReadResponsible(String key) {
        return chord.isReadResponsible(key);
    }

}
