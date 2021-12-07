package de.tum.i13.shared.hashing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.net.NetworkLocation;

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
    private final HashingAlgorithm hashingAlgorithm;
    private final NavigableMap<BigInteger, NetworkLocation> networkLocationMap;

    protected PrecedingResponsibilityHashRing(HashingAlgorithm hashingAlgorithm,
                                              NavigableMap<BigInteger, NetworkLocation> networkLocationMap) {
        this.hashingAlgorithm = hashingAlgorithm;
        this.networkLocationMap = networkLocationMap;
    }

    @Override
    public Optional<NetworkLocation> getResponsibleNetworkLocation(String key) {
        LOGGER.info("Getting {} for key '{}'", NetworkLocation.class.getSimpleName(), key);

        final BigInteger hash = hashingAlgorithm.hash(key);
        return Optional.ofNullable(networkLocationMap.ceilingEntry(hash))
                .or(() -> Optional.ofNullable(networkLocationMap.firstEntry()))
                .map(Map.Entry::getValue);
    }

    @Override
    public void addNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Adding {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = hashingAlgorithm.hash(networkLocation);
        addNetworkLocation(hash, networkLocation);
    }

    @Override
    public void addNetworkLocation(BigInteger hash, NetworkLocation networkLocation) {
        LOGGER.info("Adding {} {} at hash {}", NetworkLocation.class.getSimpleName(), networkLocation, hash);
        Optional.ofNullable(networkLocationMap.put(hash, networkLocation))
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
    public void removeNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Removing {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = hashingAlgorithm.hash(networkLocation);
        networkLocationMap.remove(hash);
    }

    @Override
    public String toString() {
        return packMessage();
    }

    @Override
    public String packMessage() {
        final NavigableMap<BigInteger, NetworkLocation> map = networkLocationMap;
        if (!map.isEmpty()) {
            final StringBuilder stringBuilder = new StringBuilder();

            final Map.Entry<BigInteger, NetworkLocation> lastEntry = map.lastEntry();
            final BigInteger lastHash = lastEntry.getKey();
            final NetworkLocation lastNetworkLocation = lastEntry.getValue();

            stringBuilder.append(HashingAlgorithm.convertHashToHex(lastHash.add(BigInteger.ONE)));

            for (Map.Entry<BigInteger, NetworkLocation> entry : map.headMap(lastHash).entrySet()) {
                final BigInteger hash = entry.getKey();
                final NetworkLocation networkLocation = entry.getValue();
                appendNetworkLocation(stringBuilder, hash, networkLocation)
                        .append(HashingAlgorithm.convertHashToHex(hash.add(BigInteger.ONE)));
            }

            return appendNetworkLocation(stringBuilder, lastHash, lastNetworkLocation)
                    .toString();
        } else return "";
    }

    private StringBuilder appendNetworkLocation(StringBuilder stringBuilder, BigInteger hash,
                                                NetworkLocation networkLocation) {
        return stringBuilder.append(",")
                .append(HashingAlgorithm.convertHashToHex(hash))
                .append(",")
                .append(networkLocation.getAddress())
                .append(":")
                .append(networkLocation.getPort())
                .append(";");
    }

    @Override
    public boolean containsNetworkLocation(NetworkLocation networkLocation) {
        return this.networkLocationMap.values().contains(networkLocation);
    }
}
