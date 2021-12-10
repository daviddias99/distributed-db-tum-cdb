package de.tum.i13.shared.hashing;

import de.tum.i13.shared.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A {@link ConsistentHashRing} that wraps around a {@link TreeMap}
 */
public class TreeMapServerMetadata extends PrecedingResponsibilityHashRing {

    private static final Logger LOGGER = LogManager.getLogger(TreeMapServerMetadata.class);

    private final HashingAlgorithm hashingAlgorithm;
    private final TreeMap<BigInteger, NetworkLocation> networkLocationMap;

    /**
     * Creates a new {@link TreeMapServerMetadata} using {@link MD5HashAlgorithm} to hash the keys and
     * {@link NetworkLocation}s.
     *
     * @see HashingAlgorithm
     * @see MD5HashAlgorithm
     */
    public TreeMapServerMetadata() {
        networkLocationMap = new TreeMap<>();
        hashingAlgorithm = new MD5HashAlgorithm();
    }

    @Override
    protected NavigableMap<BigInteger, NetworkLocation> getNavigableMap() {
        return networkLocationMap;
    }

    @Override
    public void addNetworkLocation(BigInteger hash, NetworkLocation networkLocation) {
        LOGGER.info("Adding {} {} at hash {}", NetworkLocation.class.getSimpleName(), networkLocation, hash);
        Optional.ofNullable(getNavigableMap().put(hash, networkLocation))
                .ifPresent(previousValue -> {
                    throw new IllegalStateException(
                            String.format(
                                    "Hash collision. Cannot have two network location at the same hash location %s",
                                    hash
                            ));
                });
    }

    @Override
    public HashingAlgorithm getHashingAlgorithm() {
        return hashingAlgorithm;
    }

    @Override
    public NetworkLocation getPreviousNetworkLocation(NetworkLocation location) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NetworkLocation getNextNetworkLocation(NetworkLocation location) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean contains(NetworkLocation location) {
        // TODO Auto-generated method stub
        return false;
    }


}
