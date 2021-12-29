package de.tum.i13.shared.hashing;

import java.math.BigInteger;
import java.util.List;

public interface RingRange {

    BigInteger getStart();

    BigInteger getEnd();

    boolean contains(BigInteger val);

    boolean wrapsAround();

    BigInteger getNumberOfElements();

    /**
     * Clockwise order of returned ranges
     * @param ringRange
     * @return
     */
    List<RingRange> computeDifference(RingRange ringRange);

    HashingAlgorithm getHashingAlgorithm();

}
