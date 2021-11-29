package de.tum.i13.shared.hashing;

import de.tum.i13.client.net.NetworkLocation;

/**
 * Contains the metadata of a ring based data structure that maps {@link String} keys of a key-value store to a set of
 * {@link NetworkLocation}s.
 */
public interface ConsistentHashRing {

    /**
     * Returns the {@link NetworkLocation} responsible for this key in this {@link ConsistentHashRing}.
     *
     * @param key the key of a key value pair
     * @return the {@link NetworkLocation} responsible for that key
     */
    NetworkLocation getResponsibleNetworkLocation(String key);

    /**
     * Adds a {@link NetworkLocation} to the ring.
     *
     * @param networkLocation the {@link NetworkLocation} we want to add to the {@link ConsistentHashRing}
     * @throws IllegalStateException if a {@link NetworkLocation} already in the {@link ConsistentHashRing} has the
     *                               same hash as the {@link NetworkLocation} we want to add
     */
    void addNetworkLocation(NetworkLocation networkLocation);

    /**
     * Removes a {@link NetworkLocation} from this {@link ConsistentHashRing}.
     *
     * @param networkLocation the {@link NetworkLocation} to remove
     */
    void removeNetworkLocation(NetworkLocation networkLocation);

    /**
     * Returns the hashing algorithm responsible for calculating the hashes and therefore the locations of the keys
     * and {@link NetworkLocation}s on the {@link ConsistentHashRing}
     *
     * @return the responsible hashing algorithm
     */
    HashingAlgorithm getHashingAlgorithm();

}
