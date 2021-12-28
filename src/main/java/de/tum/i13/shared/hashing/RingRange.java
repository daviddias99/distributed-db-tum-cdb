package de.tum.i13.shared.hashing;

import java.math.BigInteger;

public interface RingRange {

    BigInteger getLeft();

    BigInteger getRight();

    BigInteger getNumberOfElements();

}
