package de.tum.i13.server.state;

import de.tum.i13.server.ServerException;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.ChordException;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * State associated with a Chord Server
 */
public class ChordServerState extends AbstractServerState {

    private static final Logger LOGGER = LogManager.getLogger(ChordServerState.class);

    private final Chord chord;

    /**
     * Create new server state associated with a Chord instance
     *
     * @param chord chord instance associated with current node
     */
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

    /**
     * Get write responsible locations for given key
     *
     * @param key key to check
     * @return location responsible for key
     */
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

    @Override
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
        return chord.isReplicationActive();
    }

}
