package de.tum.i13.shared.hashing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;

import static de.tum.i13.TestUtils.checkRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RingRangeImplTest {

    RingRange ringRange;
    RingRangeFactory rangeFactory;
    @Mock
    HashingAlgorithm hashingAlgorithm;

    class RingRangeFactory {

        RingRangeFactory() {
        }

        RingRange createInstance(int start, int end) {
            return new RingRangeImpl(BigInteger.valueOf(start), BigInteger.valueOf(end), hashingAlgorithm);
        }

    }

    @Nested
    class NonWrappingRangeTest {

        @BeforeEach
        void setupDifferentRange() {
            ringRange = rangeFactory.createInstance(9, 66);
        }

        @Test
        void doesNotWrap() {
            assertThat(ringRange.wrapsAround())
                    .isFalse();
        }

        @Test
        void getsNumberOfElements() {
            assertThat(ringRange.getNumberOfElements())
                    .isEqualTo(58);
        }

        @Test
        void checksAttributesForEquality() {
            // This does not use the factory method on purpose to explicitly create a new hashing algorithm object
            final RingRangeImpl range1 = new RingRangeImpl(BigInteger.valueOf(9), BigInteger.valueOf(66),
                    new MD5HashAlgorithm());
            final RingRangeImpl range2 = new RingRangeImpl(BigInteger.valueOf(9), BigInteger.valueOf(66),
                    new MD5HashAlgorithm());
            assertThat(range1.equals(range2))
                    .isTrue();
        }

        @Test
        void doesNotSplitGettingNonWrapping() {
            assertThat(ringRange.getAsNonWrapping())
                    .satisfiesExactly(
                            range -> checkRange(range, 9, 66, hashingAlgorithm)
                    );
        }

        @Test
        void containsElementInRange() {
            assertThat(ringRange.contains(BigInteger.valueOf(14)))
                    .isTrue();
        }

        @Test
        void doesNotContainElementOutsideRange() {
            assertThat(ringRange.contains(BigInteger.valueOf(77)))
                    .isFalse();
        }

        @Test
        void computesDifferenceWithContainedRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(11, 21)))
                    .satisfiesExactly(
                            range -> checkRange(range, 9, 10, hashingAlgorithm),
                            range -> checkRange(range, 22, 66, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceContainedRangeLeftBorder() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(9, 55)))
                    .satisfiesExactly(
                            range -> checkRange(range, 56, 66, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceContainedRangeRightBorder() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(50, 66)))
                    .satisfiesExactly(
                            range -> checkRange(range, 9, 49, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(50, 3)))
                    .satisfiesExactly(
                            range -> checkRange(range, 9, 49, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceCompleteOverlapRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(9, 70)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceOverlapLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(90, 15)))
                    .satisfiesExactly(
                            range -> checkRange(range, 16, 66, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceCompleteOverlapLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(3, 66)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceOverlapLeftAndRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(44, 21)))
                    .satisfiesExactly(
                            range -> checkRange(range, 22, 43, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightBorderRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(66, 21)))
                    .satisfiesExactly(
                            range -> checkRange(range, 22, 65, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightBorderLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(44, 9)))
                    .satisfiesExactly(
                            range -> checkRange(range, 10, 43, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithSingularRangeBorderLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(9, 9)))
                    .satisfiesExactly(
                            range -> checkRange(range, 10, 66, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithSingularRangeBorderRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(66, 66)))
                    .satisfiesExactly(
                            range -> checkRange(range, 9, 65, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceNoOverlap() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(70, 90)))
                    .singleElement()
                    .isEqualTo(ringRange);
        }

        @Test
        void computesDifferenceWithBeingContained() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(5, 70)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceWithSameRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(9, 66)))
                    .isEmpty();
        }

        @Test
        void containsRangeNotBordered() {
            assertThat(ringRange.contains(rangeFactory.createInstance(10, 60)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedLeft() {
            assertThat(ringRange.contains(rangeFactory.createInstance(9, 60)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(11, 66)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedLeftAndRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(9, 66)))
                    .isTrue();
        }

        @Test
        void doesNotContainRangeOverflowingLeft() {
            assertThat(ringRange.contains(rangeFactory.createInstance(3, 60)))
                    .isFalse();
        }

        @Test
        void doesNotContainRangeOverflowingRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(9, 70)))
                    .isFalse();
        }

    }

    @BeforeEach
    void setupFactory() {
        rangeFactory = new RingRangeFactory();
    }

    @BeforeEach
    void setupsHashingAlgorithm() {
        when(hashingAlgorithm.getMax())
                .thenReturn(BigInteger.valueOf(100));
    }

    @Nested
    class WrappingRangeTest {

        @BeforeEach
        void createRingRange() {
            ringRange = rangeFactory.createInstance(77, 11);
        }

        @Test
        void splitsIntoNonWrapping() {
            assertThat(ringRange.getAsNonWrapping())
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 100, hashingAlgorithm),
                            range -> checkRange(range, 0, 11, hashingAlgorithm)
                    );
        }

        @Test
        void failsOnValueExceedingMax() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> ringRange.contains(BigInteger.valueOf(200)));
        }

        @Test
        void getsNumberOfElements() {
            assertThat(ringRange.getNumberOfElements())
                    .isEqualTo(36);
        }

        @Test
        void containsElementBeforeWrap() {
            assertThat(ringRange.contains(BigInteger.valueOf(98)))
                    .isTrue();
        }

        @Test
        void containsElementAfterWrap() {
            assertThat(ringRange.contains(BigInteger.valueOf(10)))
                    .isTrue();
        }

        @Test
        void doesNotContainElementOutsideRange() {
            assertThat(ringRange.contains(BigInteger.valueOf(13)))
                    .isFalse();
        }

        @Test
        void computesDifferenceWithContainedRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(90, 3)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 89, hashingAlgorithm),
                            range -> checkRange(range, 4, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithContainedRangeEndOnMax() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(90, 100)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 89, hashingAlgorithm),
                            range -> checkRange(range, 0, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithContainedRangeStartOnMin() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(0, 3)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 100, hashingAlgorithm),
                            range -> checkRange(range, 4, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceContainedRangeLeftBorder() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(77, 90)))
                    .satisfiesExactly(
                            range -> checkRange(range, 91, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceContainedRangeRightBorder() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(95, 11)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 94, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(95, 15)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 94, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapRightStartOnMin() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(0, 15)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 100, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceCompleteOverlapRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(77, 15)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceOverlapLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(50, 85)))
                    .satisfiesExactly(
                            range -> checkRange(range, 86, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftEndOnMax() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(50, 100)))
                    .satisfiesExactly(
                            range -> checkRange(range, 0, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceCompleteOverlapLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(70, 11)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceOverlapLeftAndRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(95, 80)))
                    .satisfiesExactly(
                            range -> checkRange(range, 81, 94, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightStartOnMin() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(0, 80)))
                    .satisfiesExactly(
                            range -> checkRange(range, 81, 100, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightEndOnMax() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(5, 100)))
                    .satisfiesExactly(
                            range -> checkRange(range, 0, 4, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightBorderRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(11, 80)))
                    .satisfiesExactly(
                            range -> checkRange(range, 81, 10, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceOverlapLeftAndRightBorderLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(8, 77)))
                    .satisfiesExactly(
                            range -> checkRange(range, 78, 7, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithBeingContained() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(70, 20)))
                    .isEmpty();
        }

        @Test
        void computesDifferenceWithSameRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(77, 11)))
                    .isEmpty();
        }

        @Test
        void containsRangeNotBordered() {
            assertThat(ringRange.contains(rangeFactory.createInstance(80, 5)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedLeft() {
            assertThat(ringRange.contains(rangeFactory.createInstance(77, 8)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(81, 11)))
                    .isTrue();
        }

        @Test
        void containsRangeBorderedLeftAndRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(77, 11)))
                    .isTrue();
        }

        @Test
        void computesDifferenceWithSingularRangeBorderLeft() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(77, 77)))
                    .satisfiesExactly(
                            range -> checkRange(range, 78, 11, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceWithSingularRangeBorderRight() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(11, 11)))
                    .satisfiesExactly(
                            range -> checkRange(range, 77, 10, hashingAlgorithm)
                    );
        }

        @Test
        void computesDifferenceNoOverlap() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(20, 50)))
                    .singleElement()
                    .isEqualTo(ringRange);
        }

        @Test
        void doesNotContainRangeOverflowingLeft() {
            assertThat(ringRange.contains(rangeFactory.createInstance(90, 15)))
                    .isFalse();
        }

        @Test
        void doesNotContainRangeOverflowingRight() {
            assertThat(ringRange.contains(rangeFactory.createInstance(60, 3)))
                    .isFalse();
        }

        @Test
        void getsStart() {
            assertThat(ringRange.getStart())
                    .isEqualTo(77);
        }

        @Test
        void getsEnd() {
            assertThat(ringRange.getEnd())
                    .isEqualTo(11);
        }

        @Test
        void doesWrap() {
            assertThat(ringRange.wrapsAround())
                    .isTrue();
        }

        @Test
        void getsHashingAlgorithm() {
            assertThat(ringRange.getHashingAlgorithm())
                    .isEqualTo(hashingAlgorithm);
        }

        @Test
        void checksAttributesForEquality() {
            // This does not use the factory method on purpose to explicitly create a new hashing algorithm object
            final RingRangeImpl range1 = new RingRangeImpl(BigInteger.valueOf(77), BigInteger.valueOf(11),
                    new MD5HashAlgorithm());
            final RingRangeImpl range2 = new RingRangeImpl(BigInteger.valueOf(77), BigInteger.valueOf(11),
                    new MD5HashAlgorithm());
            assertThat(range1.equals(range2))
                    .isTrue();
        }

    }

}