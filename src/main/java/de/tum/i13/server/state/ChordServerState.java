package de.tum.i13.server.state;

import de.tum.i13.server.ServerException;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.ChordException;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class ChordServerState extends AbstractServerState {

    private static final Logger LOGGER = LogManager.getLogger(ChordServerState.class);

    private final Chord chord;

    public ChordServerState(Chord chord) {
        super(State.ACTIVE);
        this.chord = chord;
    }

    @Override
    public boolean isWriteResponsible(String key) throws ServerException {
        try {
            return chord.isWriteResponsible(key);
        } catch (ChordException e) {
            throw new ServerException("Caught exception while checking write responsibility", e);
        }
    }

    @Override
    public boolean isReadResponsible(String key) throws ServerException {
        try {
            return chord.isReadResponsible(key);
        } catch (ChordException e) {
            throw new ServerException("Caught exception while checking read responsibility", e);
        }
    }

    public NetworkLocation getWriteResponsibleNetworkLocation(String key) {
        try {
            return chord.getWriteResponsibleNetworkLocation(key);
        } catch (ChordException e) {
            LOGGER.atFatal()
                    .withThrowable(e)
                    .log("Could not retrieve write responsible network location for key {}", key);
            return null;
        }
    }

    public List<NetworkLocation> getReadResponsibleNetworkLocation(String key) {
        try {
            return chord.getReadResponsibleNetworkLocation(key);
        } catch (ChordException e) {
            LOGGER.atFatal()
                    .withThrowable(e)
                    .log("Could not retrieve read responsible network locations for key {}", key);
            return Collections.emptyList();
        }
    }

    @Override
    public NetworkLocation getCurNetworkLocation() {
        return chord.getLocation();
    }

    @Override
    public boolean isReplicationActive() {
        return chord.getSuccessorCount() >= Constants.NUMBER_OF_REPLICAS;
    }

}
