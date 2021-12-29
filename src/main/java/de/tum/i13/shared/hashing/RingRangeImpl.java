package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Preconditions;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

class RingRangeImpl implements RingRange {

    private final BigInteger startInclusive;
    private final BigInteger endInclusive;
    private final HashingAlgorithm hashingAlgorithm;

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
    public boolean wrapsAround() {
        return startInclusive.compareTo(endInclusive) > 0;
    }

    @Override
    public BigInteger getNumberOfElements() {
        if (wrapsAround()) {
            return hashingAlgorithm.getMax().subtract(startInclusive).add(BigInteger.ONE)
                    .add(endInclusive).add(BigInteger.ONE);
        } else {
            return endInclusive.subtract(startInclusive).add(BigInteger.ONE);
        }
    }

    @Override
    public boolean contains(BigInteger val) {
        Preconditions.check(val.compareTo(hashingAlgorithm.getMax()) <= 0,
                "The value must exceed the maximum possible value");
        if (wrapsAround())
            return startInclusive.compareTo(val) <= 0 || val.compareTo(endInclusive) <= 0;
        else
            return startInclusive.compareTo(val) <= 0 && val.compareTo(endInclusive) <= 0;
    }

    @Override
    public List<RingRange> computeDifference(RingRange ringRange) {
        Preconditions.check(hashingAlgorithm.equals(ringRange.getHashingAlgorithm()), "Ranges have to use same " +
                "hashing algorithm");

        if (contains(ringRange.getStart()) && contains(ringRange.getEnd())) {
            if (wrapsAround()) {
                return List.of(
                        new RingRangeImpl(
                                ringRange.getEnd().add(BigInteger.ONE),
                                ringRange.getStart().subtract(BigInteger.ONE),
                                hashingAlgorithm
                        ));
            } else {
                return List.of(
                        new RingRangeImpl(startInclusive, ringRange.getStart().subtract(BigInteger.ONE),
                                hashingAlgorithm),
                        new RingRangeImpl(ringRange.getEnd().add(BigInteger.ONE), endInclusive, hashingAlgorithm)
                );
            }
        } else if (contains(ringRange.getStart())) {
            return List.of(
                    new RingRangeImpl(startInclusive, ringRange.getStart().subtract(BigInteger.ONE), hashingAlgorithm)
            );
        } else if (contains(ringRange.getEnd())) {
            return List.of(
                    new RingRangeImpl(ringRange.getEnd().add(BigInteger.ONE), endInclusive, hashingAlgorithm)
            );
        } else {
            return List.of();
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

}
