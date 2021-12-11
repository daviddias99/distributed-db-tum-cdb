package de.tum.i13.shared.hashing;

import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import org.apache.logging.log4j.LogManager;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Optional;

/**
 * Contains the metadata of a ring based data structure that maps {@link String} keys of a key-value store to a set of
 * {@link NetworkLocation}s.
 */
public interface ConsistentHashRing {

    /**
     * Unpacks the metadata into a {@link ConsistentHashRing}.
     * The {@link TreeMapServerMetadata} implementation is used.
     * The metadata format is specified in the specification document.
     *
     * @param metadata the metadata
     * @return a {@link ConsistentHashRing} containing the {@link NetworkLocation}s from the metadata
     */
    static ConsistentHashRing unpackMetadata(String metadata) {
        final var hashRing = new TreeMapServerMetadata();
        for (String kvStore : metadata.split(";")) {
            final String[] kvStoreData = kvStore.split(",");
            checkKVStoreData(kvStoreData);

            final String keyRangeTo = kvStoreData[1];
            final String networkLocationString = kvStoreData[2];

            final BigInteger hash = HashingAlgorithm.convertHexToHash(keyRangeTo);
            final NetworkLocation networkLocation = extractNetworkLocation(networkLocationString);

            hashRing.addNetworkLocation(hash, networkLocation);
        }
        return hashRing;
    }

    private static void checkKVStoreData(String[] kvStoreData) {
        if (kvStoreData.length != 3) {
            final IllegalArgumentException exception = new IllegalArgumentException(
                    String.format(
                            "Could not convert metadata to a '%s'. %s data '%s' was malformed",
                            ConsistentHashRing.class.getSimpleName(),
                            KVStore.class.getSimpleName(),
                            Arrays.toString(kvStoreData)
                    ));
            LogManager.getLogger(ConsistentHashRing.class)
                    .error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    private static NetworkLocation extractNetworkLocation(String networkLocationString) {
        final String[] networkLocationData = networkLocationString.split(":");
        if (networkLocationData.length != 2) {
            final IllegalArgumentException exception = new IllegalArgumentException(
                    String.format(
                            "Could not convert metadata to a '%s'. %s data '%s' was malformed",
                            ConsistentHashRing.class.getSimpleName(),
                            NetworkLocation.class.getSimpleName(),
                            Arrays.toString(networkLocationData)
                    ));
            LogManager.getLogger(ConsistentHashRing.class)
                    .error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
        final String address = networkLocationData[0];
        final String port = networkLocationData[1];

        return new NetworkLocationImpl(
                address,
                Integer.parseInt(port)
        );
    }

    /**
     * Returns the {@link NetworkLocation} responsible for this key in this {@link ConsistentHashRing}.
     * Which {@link NetworkLocation} is responsible for a given key can be determined in an implementing class.
     * Examples include the preceding or the succeeding {@link NetworkLocation}.
     *
     * @param key the key of a key value pair
     * @return an {@link Optional} containing the {@link NetworkLocation} responsible for that key.
     * Empty if the {@link ConsistentHashRing} is empty
     */
    Optional<NetworkLocation> getResponsibleNetworkLocation(String key);

    /**
     * Adds a {@link NetworkLocation} to the ring by calculating its location using
     * {@link HashingAlgorithm#hash(NetworkLocation)}.
     *
     * @param networkLocation the {@link NetworkLocation} we want to add to the {@link ConsistentHashRing}
     * @throws IllegalStateException if a {@link NetworkLocation} already in the {@link ConsistentHashRing} has the
     *                               same hash as the {@link NetworkLocation} we want to add
     */
    void addNetworkLocation(NetworkLocation networkLocation);

    /**
     * Adds a {@link NetworkLocation} to the ring at the specified hash location.
     *
     * @param networkLocation the {@link NetworkLocation} we want to add to the {@link ConsistentHashRing}
     * @param hash            the hash location on the ring where to add the {@link NetworkLocation}
     * @throws IllegalStateException if a {@link NetworkLocation} already in the {@link ConsistentHashRing} has the
     *                               same hash as the {@link NetworkLocation} we want to add
     */
    void addNetworkLocation(BigInteger hash, NetworkLocation networkLocation);

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

    /**
     * Packs the metadata of this {@link ConsistentHashRing} into a {@link String}.
     * The metadata format is specified in the specification document.
     * The ranges from and to key range values are both inclusive.
     *
     * @return a {@link String} representation of this {@link ConsistentHashRing}
     */
    String packMessage();

    /**
     * Returns the {@link NetworkLocation} that succeeds the supplied {@link NetworkLocation} in the
     * {@link ConsistentHashRing}. The supplied {@link NetworkLocation} does not have to be contained in the
     * {@link ConsistentHashRing}.
     * <p>
     * If the {@link ConsistentHashRing} is empty, an empty {@link Optional} is returned.
     * If the supplied {@link NetworkLocation} is already contained in the {@link ConsistentHashRing} and the only
     * element in
     * that {@link ConsistentHashRing} also an empty {@link Optional} is returned.
     *
     * @param location the {@link NetworkLocation} whose successor is to be determined
     * @return the successor of the supplied {@link NetworkLocation} or an empty {@link Optional}, if the
     * {@link ConsistentHashRing} is empty or the supplied {@link NetworkLocation} is the only element
     * @see #getPrecedingNetworkLocation(NetworkLocation)
     */
    Optional<NetworkLocation> getSucceedingNetworkLocation(NetworkLocation location);

    /**
     * Returns the {@link NetworkLocation} that precedes the supplied {@link NetworkLocation} in the
     * {@link ConsistentHashRing}. The supplied {@link NetworkLocation} does not have to be contained in the
     * {@link ConsistentHashRing}.
     * <p>
     * If the {@link ConsistentHashRing} is empty, an empty {@link Optional} is returned.
     * If the supplied {@link NetworkLocation} is already in the {@link ConsistentHashRing} and the only element in
     * that {@link ConsistentHashRing} also an empty {@link Optional} is returned.
     *
     * @param location the {@link NetworkLocation} whose predecessor is to be determined
     * @return the predecessor of the supplied {@link NetworkLocation} or an empty {@link Optional}, if the
     * {@link ConsistentHashRing} is empty or the supplied {@link NetworkLocation} is the only element
     * @see #getSucceedingNetworkLocation(NetworkLocation)
     */
    Optional<NetworkLocation> getPrecedingNetworkLocation(NetworkLocation location);

    /**
     * Returns whether the supplied {@link NetworkLocation} is contained in the given {@link ConsistentHashRing}.
     *
     * @param location the {@link NetworkLocation} to check for in the {@link ConsistentHashRing}
     * @return whether the supplied {@link NetworkLocation} is contained in the {@link ConsistentHashRing}
     */
    boolean contains(NetworkLocation location);

}
