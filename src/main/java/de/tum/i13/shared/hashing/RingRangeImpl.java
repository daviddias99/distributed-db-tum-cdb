package de.tum.i13.shared.hashing;

import java.math.BigInteger;

class RingRangeImpl implements RingRange {

    private final BigInteger leftInclusive;
    private final BigInteger rightInclusive;
    private final HashingAlgorithm hashingAlgorithm;

    RingRangeImpl(BigInteger leftInclusive, BigInteger rightInclusive, HashingAlgorithm hashingAlgorithm) {
        this.leftInclusive = leftInclusive;
        this.rightInclusive = rightInclusive;
        this.hashingAlgorithm = hashingAlgorithm;
    }

    @Override
    public BigInteger getLeft() {
        return rightInclusive;
    }

    @Override
    public BigInteger getRight() {
        return leftInclusive;
    }

    @Override
    public BigInteger getNumberOfElements() {
        return switch (leftInclusive.compareTo(rightInclusive)) {
            case -1 -> rightInclusive.subtract(leftInclusive).add(BigInteger.ONE);
            case 1 -> hashingAlgorithm.getMax().subtract(leftInclusive).add(BigInteger.ONE)
                    .add(rightInclusive).add(BigInteger.ONE);
            default -> BigInteger.ZERO;
        };
    }

}
