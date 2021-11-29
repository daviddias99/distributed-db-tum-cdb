package de.tum.i13.shared.hashing;

import de.tum.i13.client.net.NetworkLocation;

import java.math.BigInteger;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A {@link ConsistentHashRing} that wraps around a {@link TreeMap}
 */
public class TreeMapServerMetadata extends PrecedingResponsibilityHashRing implements ConsistentHashRing {

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
    public HashingAlgorithm getHashingAlgorithm() {
        return hashingAlgorithm;
    }


}
