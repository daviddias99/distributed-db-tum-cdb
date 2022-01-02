package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

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
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
        this.hashingAlgorithm = hashingAlgorithm;
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
        LOGGER.debug("Checking presence of value {} in {}", value, this);
        Preconditions.check(value.compareTo(hashingAlgorithm.getMax()) <= 0,
                () -> String.format("The value %s must not exceed the maximum possible value %s",
                        value, hashingAlgorithm.getMax()));
        return wrapsAround() && (startInclusive.compareTo(value) <= 0 || value.compareTo(endInclusive) <= 0)
                || startInclusive.compareTo(value) <= 0 && value.compareTo(endInclusive) <= 0;
    }

    @Override
    public List<RingRange> computeDifference(RingRange ringRange) {
        LOGGER.debug("Computing the difference of {} without {}", this, ringRange);
        Preconditions.check(hashingAlgorithm.equals(ringRange.getHashingAlgorithm()), "Ranges have to use same " +
                "hashing algorithm");

        if (equals(ringRange)) return List.of();
        else if (contains(ringRange)) return computeDifferenceContainedRange(ringRange);
        else if (overlapsLeftAndRight(ringRange)) return computeDifferenceOverlapLeftAndRight(ringRange);
        else if (contains(ringRange.getStart())) return computeDifferenceOverlapRight(ringRange);
        else if (contains(ringRange.getEnd())) return computeDifferenceOverlapLeft(ringRange);
        else return computerDifferenceNoOverlapOrContainment(ringRange);
    }

    private List<RingRange> computerDifferenceNoOverlapOrContainment(RingRange ringRange) {
        LOGGER.trace("{} does not overlap with or is being contained by {}", this, ringRange);
        return List.of();
    }

    private List<RingRange> computeDifferenceOverlapLeft(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the left with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(ringRange.getEnd().add(BigInteger.ONE), endInclusive, hashingAlgorithm)
        );
    }

    private List<RingRange> computeDifferenceOverlapRight(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the right with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(startInclusive, ringRange.getStart().subtract(BigInteger.ONE), hashingAlgorithm)
        );
    }

    private List<RingRange> computeDifferenceOverlapLeftAndRight(RingRange ringRange) {
        LOGGER.trace("{} overlaps on the left and right with {}", this, ringRange);
        return List.of(
                new RingRangeImpl(
                        ringRange.getEnd().add(BigInteger.ONE),
                        ringRange.getStart().subtract(BigInteger.ONE),
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
                    new RingRangeImpl(startInclusive, ringRange.getStart().subtract(BigInteger.ONE),
                            hashingAlgorithm),
                    new RingRangeImpl(ringRange.getEnd().add(BigInteger.ONE), endInclusive, hashingAlgorithm)
            );
        }
    }

    private boolean contains(RingRange ringRange) {
        return contains(ringRange.getStart()) && contains(ringRange.getEnd())
                && indexOf(ringRange.getStart()).compareTo(indexOf(ringRange.getEnd())) <= 0;
    }

    private boolean overlapsLeftAndRight(RingRange ringRange) {
        return contains(ringRange.getStart()) && contains(ringRange.getEnd())
                && indexOf(ringRange.getStart()).compareTo(indexOf(ringRange.getEnd())) > 0;
    }

    private BigInteger indexOf(BigInteger value) {
        Preconditions.check(contains(value), () -> String.format("The value %s must be contained in the range %s",
                value, this));
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
        return String.format("RingRangeImpl{%s,%s}", startInclusive, endInclusive);
    }

}
