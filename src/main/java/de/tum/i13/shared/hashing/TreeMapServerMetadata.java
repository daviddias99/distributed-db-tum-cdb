package de.tum.i13.shared.hashing;

import de.tum.i13.shared.net.NetworkLocation;

import java.util.TreeMap;

/**
 * A {@link ConsistentHashRing} that wraps around a {@link TreeMap}
 */
public class TreeMapServerMetadata extends PrecedingResponsibilityHashRing {

    /**
     * Creates a new {@link TreeMapServerMetadata} using {@link MD5HashAlgorithm} to hash the keys and
     * {@link NetworkLocation}s.
     *
     * @see HashingAlgorithm
     * @see MD5HashAlgorithm
     */
    public TreeMapServerMetadata() {
        super(new MD5HashAlgorithm(), new TreeMap<>());
    }

    // TODO Change copying
    public TreeMapServerMetadata(TreeMapServerMetadata copy) {
        super(copy.getHashingAlgorithm(), new TreeMap<>(copy.getNetworkLocationMap()));
    }
}
