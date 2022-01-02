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


    abstract class NonWrappingRange {

        @BeforeEach
        void setupDifferentRange() {
            ringRange = rangeFactory.createInstance(9, 66);
        }

    }

    abstract class WrappingRange {

        @BeforeEach
        void createRingRange() {
            ringRange = rangeFactory.createInstance(77, 11);
        }

    }

    @BeforeEach
    void setupFactory() {
        rangeFactory = new RingRangeFactory();
    }

    @Nested
    class WrappingRangeTest extends WrappingRange {

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
        void computesDifferenceWithSameRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(77, 11)))
                    .isEmpty();
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


    @Nested
    class NonWrappingRangeTest extends NonWrappingRange {

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
        void computesDifferenceWithSameRange() {
            assertThat(ringRange.computeDifference(rangeFactory.createInstance(9, 66)))
                    .isEmpty();
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

    }

    @Nested
    class ChecksMaxTest {

        @BeforeEach
        void setupsHashingAlgorithm() {
            when(hashingAlgorithm.getMax())
                    .thenReturn(BigInteger.valueOf(100));
        }

        @Nested
        class WrappingRangeTest extends WrappingRange {

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
            void computesDifferenceOverlapLeft() {
                assertThat(ringRange.computeDifference(rangeFactory.createInstance(50, 85)))
                        .satisfiesExactly(
                                range -> checkRange(range, 86, 11, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapLeftAndRight() {
                assertThat(ringRange.computeDifference(rangeFactory.createInstance(95, 80)))
                        .satisfiesExactly(
                                range -> checkRange(range, 81, 94, hashingAlgorithm)
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

        }

        @Nested
        class NonWrappingRangeTest extends NonWrappingRange {

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
            void computesDifferenceOverlapLeft() {
                assertThat(ringRange.computeDifference(rangeFactory.createInstance(90, 15)))
                        .satisfiesExactly(
                                range -> checkRange(range, 16, 66, hashingAlgorithm)
                        );
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
            void computesDifferenceWithBeingContained() {
                assertThat(ringRange.computeDifference(rangeFactory.createInstance(5, 70)))
                        .isEmpty();
            }

        }

    }

}