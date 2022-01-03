package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static de.tum.i13.shared.hashing.HashingAlgorithm.convertHashToHex;

/**
 * A standard implementation of a {@link RingRange}
 */

class RingRangeImpl implements RingRange {

    private static final Logger LOGGER = LogManager.getLogger(RingRangeImpl.class);

    private final BigInteger startInclusive;
    private final BigInteger endInclusive;
    private final HashingAlgorithm hashingAlgorithm;

    /**
     * Creates a new {@link RingRangeImpl} with the associated value
     *
     * @param startInclusive   the start of {@link RingRange}
     * @param endInclusive     the end of the {@link RingRange}
     * @param hashingAlgorithm the {@link HashingAlgorithm} associated with the {@link RingRange}
     */
    RingRangeImpl(BigInteger startInclusive, BigInteger endInclusive, HashingAlgorithm hashingAlgorithm) {
        Preconditions.notNull(startInclusive, "The start must not be null");
        Preconditions.notNull(endInclusive, "The end must not be null");
        Preconditions.notNull(hashingAlgorithm, "The hashing algorithm must not be null");

        this.hashingAlgorithm = hashingAlgorithm;

        checkInBounds(startInclusive, "start");
        checkInBounds(endInclusive, "end");

        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    private void checkInBounds(BigInteger value, String parameterName) {
        Preconditions.check(value.compareTo(BigInteger.ZERO) >= 0,
                () -> String.format("The %s %s must be greater or equal to 0", parameterName, convertHashToHex(value)));
        Preconditions.check(value.compareTo(hashingAlgorithm.getMax()) <= 0,
                () -> String.format("The %s %s must be less or equal to the max %s", parameterName,
                        convertHashToHex(value), convertHashToHex(hashingAlgorithm.getMax())));
    }

    @Override
    public BigInteger getStart() {
        return startInclusive;
    }

    @Override
    public BigInteger getEnd() {
        return endInclusive;
    }

    @Override
    public HashingAlgorithm getHashingAlgorithm() {
        return hashingAlgorithm;
    }

    @Override
    public List<RingRange> getAsNonWrapping() {
        return wrapsAround()
                ? List.of(
                new RingRangeImpl(getStart(), hashingAlgorithm.getMax(), hashingAlgorithm),
                new RingRangeImpl(BigInteger.ZERO, getEnd(), hashingAlgorithm)
        ) : List.of(this);
    }

    @Override
    public boolean wrapsAround() {
        LOGGER.trace("Checking wrapping behavior of {}", this);
        return startInclusive.compareTo(endInclusive) > 0;
    }

    @Override
    public BigInteger getNumberOfElements() {
        LOGGER.debug("Getting the number of elements of {}", this);
        if (wrapsAround()) {
            return hashingAlgorithm.getMax().subtract(startInclusive).add(BigInteger.ONE)
                    .add(endInclusive).add(BigInteger.ONE);
        } else {
            return endInclusive.subtract(startInclusive).add(BigInteger.ONE);
        }
    }

    @Override
    public boolean contains(BigInteger value) {
        LOGGER.debug("Checking presence of value {} in {}", () -> convertHashToHex(value), () -> this);
        checkInBounds(value, "value");
        return wrapsAround() && (startInclusive.compareTo(value) <= 0 || value.compareTo(endInclusive) <= 0)
                || startInclusive.compareTo(value) <= 0 && value.compareTo(endInclusive) <= 0;
    }

    @Override
    public List<RingRange> computeDifference(RingRange ringRange) {
        LOGGER.debug("Computing the difference of {} without {}", this, ringRange);
        Preconditions.check(hashingAlgorithm.equals(ringRange.getHashingAlgorithm()),
                String.format("Ranges %s and %s have to use the same hashing algorithm", this, ringRange));

        if (coversWholeRing(ringRange) || ringRange.contains(this)) return List.of();
        else if (contains(ringRange)) return computeDifferenceContainedRange(ringRange);
        else if (overlapsLeftAndRight(ringRange)) return computeDifferenceOverlapLeftAndRight(ringRange);
        else if (contains(ringRange.getStart())) return computeDifferenceOverlapRight(ringRange);
        else if (contains(ringRange.getEnd())) return computeDifferenceOverlapLeft(ringRange);
        else return computeDifferenceNoOverlap(ringRange);
    }

    private boolean coversWholeRing(RingRange ringRange) {
        return decrement(ringRange.getStart()).equals(ringRange.getEnd());
    }

    private List<RingRange> computeDifferenceNoOverlap(RingRange ringRange) {
        LOGGER.trace("{} does not overlap at all with {}", this, ringRange);
        return List.of(this);
    }

    private List<RingRange> computeDifferenceOverlapLeft(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the left with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(increment(ringRange.getEnd()), endInclusive, hashingAlgorithm)
        );
    }

    private List<RingRange> computeDifferenceOverlapRight(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the right with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(startInclusive, decrement(ringRange.getStart()), hashingAlgorithm)
        );
    }

    private List<RingRange> computeDifferenceOverlapLeftAndRight(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the left and right with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(
                        increment(ringRange.getEnd()),
                        decrement(ringRange.getStart()),
                        hashingAlgorithm
                ));
    }

    private List<RingRange> computeDifferenceContainedRange(RingRange ringRange) {
        LOGGER.trace("{} contains {}", this, ringRange);
        if (getStart().equals(ringRange.getStart())) {
            return computeDifferenceOverlapLeft(ringRange);
        } else if (getEnd().equals(ringRange.getEnd())) {
            return computeDifferenceOverlapRight(ringRange);
        } else {
            return List.of(
                    new RingRangeImpl(startInclusive, decrement(ringRange.getStart()),
                            hashingAlgorithm),
                    new RingRangeImpl(increment(ringRange.getEnd()), endInclusive, hashingAlgorithm)
            );
        }
    }

    private BigInteger decrement(BigInteger value) {
        if (value.equals(BigInteger.ZERO)) return hashingAlgorithm.getMax();
        else return value.subtract(BigInteger.ONE);
    }

    private BigInteger increment(BigInteger value) {
        if (value.equals(hashingAlgorithm.getMax())) return BigInteger.ZERO;
        else return value.add(BigInteger.ONE);
    }

    @Override
    public boolean contains(RingRange ringRange) {
        return contains(ringRange.getStart()) && contains(ringRange.getEnd())
                && indexOf(ringRange.getStart()).compareTo(indexOf(ringRange.getEnd())) <= 0;
    }

    private boolean overlapsLeftAndRight(RingRange ringRange) {
        return contains(ringRange.getStart()) && contains(ringRange.getEnd())
                && indexOf(ringRange.getStart()).compareTo(indexOf(ringRange.getEnd())) > 0;
    }

    private BigInteger indexOf(BigInteger value) {
        Preconditions.check(contains(value), () -> String.format("The value %s must be contained in the range %s",
                convertHashToHex(value), this));
        if (wrapsAround() && value.compareTo(endInclusive) <= 0) {
            return hashingAlgorithm.getMax()
                    .subtract(startInclusive)
                    .add(BigInteger.ONE)
                    .add(value);
        } else {
            return value.subtract(startInclusive);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RingRangeImpl)) return false;
        RingRangeImpl ringRange = (RingRangeImpl) o;
        return Objects.equals(startInclusive, ringRange.startInclusive) && Objects.equals(endInclusive,
                ringRange.endInclusive) && Objects.equals(hashingAlgorithm, ringRange.hashingAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startInclusive, endInclusive, hashingAlgorithm);
    }

    @Override
    public String toString() {
        return String.format(
                "RingRangeImpl{%s,%s}",
                convertHashToHex(startInclusive),
                convertHashToHex(endInclusive)
        );
    }

}
