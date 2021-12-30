package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Preconditions;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.tum.i13.shared.hashing.HashingAlgorithm.convertHashToHex;

/**
 * A partial implementation of {@link ConsistentHashRing} using a {@link NavigableMap} that provides helper methods
 * to minimize the effort to implement the {@link ConsistentHashRing} interface. All methods in this class are
 * synchronized. Thus, it can be used in a concurrent context.
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

    protected PrecedingResponsibilityHashRing(PrecedingResponsibilityHashRing hashRingToCopy) {
        this.hashingAlgorithm = hashRingToCopy.hashingAlgorithm;
        this.networkLocationMap = new TreeMap<>(hashRingToCopy.networkLocationMap);
    }

    @Override
    public synchronized Optional<NetworkLocation> getWriteResponsibleNetworkLocation(String key) {
        LOGGER.info("Getting write responsible {} for key '{}'", NetworkLocation.class.getSimpleName(), key);

        final BigInteger hash = hashingAlgorithm.hash(key);
        return Optional.ofNullable(networkLocationMap.ceilingEntry(hash))
                .or(() -> Optional.ofNullable(networkLocationMap.firstEntry()))
                .map(Map.Entry::getValue);
    }

    @SuppressWarnings("java:S2184")
    @Override
    public Optional<NetworkLocation> getReadResponsibleNetworkLocation(String key) {
        final Optional<NetworkLocation> writeResponsibleNetworkLocation = getWriteResponsibleNetworkLocation(key);
        if (writeResponsibleNetworkLocation.isEmpty())
            return Optional.empty();

        if (!isReplicationActive())
            return writeResponsibleNetworkLocation;


        return Stream.iterate(writeResponsibleNetworkLocation.get(),
                        iterLocation -> getSucceedingNetworkLocation(iterLocation)
                                .orElseThrow(() -> new IllegalStateException("Could not get succeeding location"))
                )
                .limit(Constants.NUMBER_OF_REPLICAS + 1)
                .skip(new Random().nextInt(Constants.NUMBER_OF_REPLICAS + 1))
                .findFirst();
    }

    @Override
    public synchronized void addNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Adding {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = hashingAlgorithm.hash(networkLocation);
        addNetworkLocation(hash, networkLocation);
    }

    @Override
    public synchronized void addNetworkLocation(BigInteger hash, NetworkLocation networkLocation) {
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
    public synchronized HashingAlgorithm getHashingAlgorithm() {
        return hashingAlgorithm;
    }

    @Override
    public synchronized void removeNetworkLocation(NetworkLocation networkLocation) {
        LOGGER.info("Removing {} '{}'", NetworkLocation.class.getSimpleName(), networkLocation);

        final BigInteger hash = hashingAlgorithm.hash(networkLocation);
        networkLocationMap.remove(hash);
    }

    @Override
    public synchronized String toString() {
        return packWriteRanges();
    }

    @Override
    public synchronized String packWriteRanges() {
        final NavigableMap<BigInteger, NetworkLocation> map = networkLocationMap;
        if (!map.isEmpty()) {
            final StringBuilder stringBuilder = new StringBuilder();

            final Map.Entry<BigInteger, NetworkLocation> lastEntry = map.lastEntry();
            final BigInteger lastHash = lastEntry.getKey();
            final NetworkLocation lastNetworkLocation = lastEntry.getValue();

            stringBuilder.append(convertHashToHex(lastHash.add(BigInteger.ONE)));

            for (Map.Entry<BigInteger, NetworkLocation> entry : map.headMap(lastHash).entrySet()) {
                final BigInteger hash = entry.getKey();
                final NetworkLocation networkLocation = entry.getValue();
                appendNetworkLocation(stringBuilder, hash, networkLocation)
                        .append(convertHashToHex(hash.add(BigInteger.ONE)));
            }

            return appendNetworkLocation(stringBuilder, lastHash, lastNetworkLocation)
                    .toString();
        } else return "";
    }

    // TODO Synchronize all methods
    @Override
    public synchronized String packReadRanges() {
        return getAllNetworkLocations().stream()
                .map(networkLocation -> {
                    final RingRange readRange = getReadRange(networkLocation);
                    return String.format(
                            "%s,%s,%s:%s;",
                            convertHashToHex(readRange.getStart()), convertHashToHex(readRange.getEnd()),
                            networkLocation.getAddress(),
                            networkLocation.getPort()
                    );
                })
                .collect(Collectors.joining(","));
    }

    private StringBuilder appendNetworkLocation(StringBuilder stringBuilder, BigInteger hash,
                                                NetworkLocation networkLocation) {
        return stringBuilder.append(",")
                .append(convertHashToHex(hash))
                .append(",")
                .append(networkLocation.getAddress())
                .append(":")
                .append(networkLocation.getPort())
                .append(";");
    }

    @Override
    public synchronized boolean contains(NetworkLocation location) {
        return networkLocationMap.containsKey(hashingAlgorithm.hash(location));
    }

    @Override
    public List<NetworkLocation> getAllNetworkLocations() {
        return List.copyOf(networkLocationMap.values());
    }

    @Override
    public synchronized Optional<NetworkLocation> getSucceedingNetworkLocation(NetworkLocation location) {
        LOGGER.info("Getting succeeding {} for {} '{}'", NetworkLocation.class.getSimpleName(),
                NetworkLocation.class.getSimpleName(), location);

        final BigInteger hash = hashingAlgorithm.hash(location);
        return Optional.ofNullable(networkLocationMap.higherEntry(hash))
                .or(() -> Optional.ofNullable(networkLocationMap.firstEntry()))
                .map(Map.Entry::getValue)
                .filter(foundLocation -> !foundLocation.equals(location));
    }

    @Override
    public synchronized Optional<NetworkLocation> getPrecedingNetworkLocation(NetworkLocation location) {
        LOGGER.info("Getting preceding {} for {} '{}'", NetworkLocation.class.getSimpleName(),
                NetworkLocation.class.getSimpleName(), location);

        final BigInteger hash = hashingAlgorithm.hash(location);
        return Optional.ofNullable(networkLocationMap.lowerEntry(hash))
                .or(() -> Optional.ofNullable(networkLocationMap.lastEntry()))
                .map(Map.Entry::getValue)
                .filter(foundLocation -> !foundLocation.equals(location));
    }

    @Override
    public synchronized int size() {
        return networkLocationMap.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public synchronized boolean isWriteResponsible(NetworkLocation networkLocation, String key) {
        checkPresence(networkLocation);

        return getWriteResponsibleNetworkLocation(key)
                .map(responsibleLocation -> responsibleLocation.equals(networkLocation))
                .orElse(false);
    }

    @Override
    public synchronized boolean isReadResponsible(NetworkLocation networkLocation, String key) {
        checkPresence(networkLocation);

        // If replication is not active, we do not have to check other network location, but the given one
        if (!isReplicationActive())
            return isWriteResponsible(networkLocation, key);

        // Check whether the key should be replicated on the location from one of the preceding locations
        // Or whether the location itself is write-responsible
        return getReplicatedLocations(networkLocation)
                .anyMatch(iterLocation -> isWriteResponsible(iterLocation, key));
    }

    @SuppressWarnings("java:S2184")
    private Stream<NetworkLocation> getReplicatedLocations(NetworkLocation networkLocation) {
        return Stream.iterate(networkLocation,
                        iterLocation -> getPrecedingNetworkLocation(iterLocation)
                                .orElseThrow(() -> new IllegalStateException("Could not get preceding location"))
                )
                .limit(Constants.NUMBER_OF_REPLICAS + 1);
    }

    @Override
    public synchronized RingRange getWriteRange(NetworkLocation networkLocation) {
        checkPresence(networkLocation);

        final BigInteger networkLocationHash = hashingAlgorithm.hash(networkLocation);

        if (size() < 2)
            return new RingRangeImpl(networkLocationHash.add(BigInteger.ONE), networkLocationHash, hashingAlgorithm);

        final BigInteger leftLocationHash = Optional.ofNullable(networkLocationMap.lowerKey(networkLocationHash))
                .or(() -> Optional.ofNullable(networkLocationMap.lastKey()))
                .orElseThrow(() -> new IllegalStateException("Could not find range start in ring with at least 2 " +
                        "members"));
        return new RingRangeImpl(leftLocationHash.add(BigInteger.ONE), networkLocationHash, hashingAlgorithm);
    }

    @Override
    public RingRange getReadRange(NetworkLocation networkLocation) {
        checkPresence(networkLocation);

        if (!isReplicationActive())
            return getWriteRange(networkLocation);

        final NetworkLocation firstReplicatedLocation = getReplicatedLocations(networkLocation)
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        list -> {
                            Collections.reverse(list);
                            return list;
                        }
                ))
                .get(0);
        final BigInteger startHash = getPrecedingNetworkLocation(firstReplicatedLocation)
                .map(hashingAlgorithm::hash)
                .map(hash -> hash.add(BigInteger.ONE))
                .orElseThrow(() -> new IllegalStateException("Could not find start hash"));

        return new RingRangeImpl(startHash, hashingAlgorithm.hash(networkLocation), hashingAlgorithm);
    }

    @Override
    public boolean isReplicationActive() {
        return size() > Constants.NUMBER_OF_REPLICAS;
    }

    private void checkPresence(NetworkLocation networkLocation) {
        Preconditions.check(contains(networkLocation),
                String.format("The %s '%s' must be contained in the %s",
                        NetworkLocation.class.getSimpleName(), networkLocation,
                        ConsistentHashRing.class.getSimpleName()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrecedingResponsibilityHashRing)) return false;
        PrecedingResponsibilityHashRing that = (PrecedingResponsibilityHashRing) o;
        return Objects.equals(getHashingAlgorithm(), that.getHashingAlgorithm())
                && Objects.equals(networkLocationMap, that.networkLocationMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getHashingAlgorithm(), networkLocationMap);
    }

}
