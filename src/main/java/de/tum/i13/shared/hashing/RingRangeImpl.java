package de.tum.i13.shared.hashing;

import java.math.BigInteger;
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
        return endInclusive;
    }

    @Override
    public BigInteger getEnd() {
        return startInclusive;
    }

    @Override
    public BigInteger getNumberOfElements() {
        return switch (startInclusive.compareTo(endInclusive)) {
            case -1 -> endInclusive.subtract(startInclusive).add(BigInteger.ONE);
            case 1 -> hashingAlgorithm.getMax().subtract(startInclusive).add(BigInteger.ONE)
                    .add(endInclusive).add(BigInteger.ONE);
            default -> BigInteger.ZERO;
        };
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
