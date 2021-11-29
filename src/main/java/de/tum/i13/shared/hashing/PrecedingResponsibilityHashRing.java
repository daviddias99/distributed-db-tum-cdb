package de.tum.i13.shared.hashing;

import de.tum.i13.client.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * A partial implementation of {@link ConsistentHashRing} using a {@link NavigableMap} that provides helper methods
 * to minimize the effort to implement the {@link ConsistentHashRing} interface.
 * <p>
 * In this implementation a {@link NetworkLocation} is responsible for all keys between its position (inclusive) and
 * the position of its predecessor (exclusive) in the ring.
 */
public abstract class PrecedingResponsibilityHashRing implements ConsistentHashRing {

    private static final Logger LOGGER = LogManager.getLogger(PrecedingResponsibilityHashRing.class);

    /**
     * Returns the {@link NavigableMap} used by this {@link ConsistentHashRing}
     *
     * @return the used {@link NavigableMap}
     */
    protected abstract NavigableMap<BigInteger, NetworkLocation> getNavigableMap();

    @Override
    public Optional<NetworkLocation> getResponsibleNetworkLocation(String key) {
        LOGGER.info("Getting {} for key '{}'", NetworkLocation.class.getSimpleName(), key);

        final BigInteger hash = getHashingAlgorithm().hash(key);
        return Optional.ofNullable(getNavigableMap().ceilingEntry(hash))
                .or(() -> Optional.ofNullable(getNavigableMap().firstEntry()))
                .map(Map.Entry::getValue);
    }

    @Override
    public void addNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Adding {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = getHashingAlgorithm().hash(networkLocation);
        Optional.ofNullable(getNavigableMap().put(hash, networkLocation))
                .ifPresent(previousValue -> {
                    throw new IllegalStateException(
                            String.format(
                                    "Hash collision. Cannot have two network location at the same hash location %s",
                                    hash)
                    );
                });
    }

    @Override
    public void removeNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Removing {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = getHashingAlgorithm().hash(networkLocation);
        getNavigableMap().remove(hash);
    }

}
