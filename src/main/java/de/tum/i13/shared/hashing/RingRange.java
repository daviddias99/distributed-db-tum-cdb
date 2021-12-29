package de.tum.i13.shared.hashing;

import java.math.BigInteger;

public interface RingRange {

    BigInteger getStart();

    BigInteger getEnd();

    BigInteger getNumberOfElements();

}
