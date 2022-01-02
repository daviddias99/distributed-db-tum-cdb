package de.tum.i13.shared.hashing;

import java.math.BigInteger;
import java.util.List;

/**
 * Represents a range of elements in a {@link ConsistentHashRing} by an inclusive start and end element.
 * A range is always associated with a {@link HashingAlgorithm} because it specifies the maximum possible value
 * and therefore determines computations in the methods of the range, i.e. in {@link #getNumberOfElements()} or
 * {@link #computeDifference(RingRange)}
 * <p>
 * Note: All ranges are non-empty. Empty ranges cannot be represented.
 */
public interface RingRange {

    /**
     * Returns the start element of the {@link RingRange}. This element is inclusive.
     *
     * @return the start element of the {@link RingRange}
     */
    BigInteger getStart();

    /**
     * Returns the end element of the {@link RingRange}. This element is inclusive.
     *
     * @return the start element of the {@link RingRange}
     */
    BigInteger getEnd();

    /**
     * Returns whether the supplied value is contained in the {@link RingRange}.
     *
     * @param value the value to check for
     * @return whether the value is in contained in the {@link RingRange}
     */
    boolean contains(BigInteger value);

    boolean contains(RingRange value);

    /**
     * Returns whether the {@link RingRange} wraps around the connection of the highest and lowest value in the
     * {@link ConsistentHashRing}.
     *
     * @return whether the {@link RingRange} wraps around the start and end of the {@link ConsistentHashRing}
     */
    boolean wrapsAround();

    /**
     * Returns the number of elements that this {@link RingRange} covers on the {@link ConsistentHashRing}.
     * Note that by covering, it is not meant, that these elements are contained in the {@link ConsistentHashRing},
     * but that they possibly could be contained in the {@link ConsistentHashRing}.
     *
     * @return the number of elements in the {@link ConsistentHashRing} covered by this {@link RingRange}
     */
    BigInteger getNumberOfElements();

    /**
     * Computes the difference between this {@link RingRange}, say {@code A}, and that supplied {@link RingRange},
     * say {@code B}, by computing the mathematical set difference {@code A\B}.
     * If the set difference cannot be represented by one {@link RingRange}, the {@link RingRange}s to represent the
     * difference are returned in clockwise order of the {@link ConsistentHashRing}.
     *
     * @param ringRange the {@link RingRange} with which to compute the difference (i.e. {@code B})
     * @return the difference between this {@link RingRange} and the supplied {@link RingRange} represented by a
     * {@link List} of {@link RingRange}s returned in clockwise order
     */
    List<RingRange> computeDifference(RingRange ringRange);

    /**
     * Returns the {@link HashingAlgorithm} associated with this {@link RingRange} as describe on the class
     * documentation
     * of {@link RingRange}.
     *
     * @return the associated {@link RingRange}
     */
    HashingAlgorithm getHashingAlgorithm();

    /**
     * Returns the range as a {@link List} of non-wrapping {@link RingRange}s.
     * <p>
     * If this range is non-wrapping the list only contains this range.
     * If this range is wrapping the list contains a range from the start of this {@link RingRange} to the maximum
     * value of the {@link ConsistentHashRing} and from the start/origin of the {@link ConsistentHashRing} to the end
     * of this {@link RingRange}.
     *
     * @return the range as a {@link List} of non-wrapping {@link RingRange}s
     * @see #wrapsAround()
     * @see HashingAlgorithm#getMax()
     */
    List<RingRange> getAsNonWrapping();

}
