package de.tum.i13.server.state;

import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.shared.net.NetworkLocation;

public class ChordServerState extends AbstractServerState implements ServerState {

    private NetworkLocation curLocation;
    private Chord chord;

    public ChordServerState(NetworkLocation curLocation, Chord chord) {
        super( State.ACTIVE);
        this.curLocation = curLocation;
        this.chord = chord;
    }

    @Override
    public boolean responsibleForKey(String key) {
        throw new UnsupportedOperationException();
    }

}
