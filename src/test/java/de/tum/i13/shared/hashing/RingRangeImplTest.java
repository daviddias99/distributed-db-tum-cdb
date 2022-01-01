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
    @Mock
    HashingAlgorithm hashingAlgorithm;

    abstract class NonWrappingRange {

        @BeforeEach
        void setupDifferentRange() {
            ringRange = new RingRangeImpl(BigInteger.valueOf(9), BigInteger.valueOf(66), hashingAlgorithm);
        }

    }

    abstract class WrappingRange {

        @BeforeEach
        void createRingRange() {
            ringRange = new RingRangeImpl(BigInteger.valueOf(77), BigInteger.valueOf(11), hashingAlgorithm);
        }

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
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(90), BigInteger.valueOf(3),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 77, 89, hashingAlgorithm),
                                range -> checkRange(range, 4, 11, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapRight() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(95), BigInteger.valueOf(15),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 77, 94, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapLeft() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(50), BigInteger.valueOf(85),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 86, 11, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapLeftAndRight() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(95), BigInteger.valueOf(80),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 81, 94, hashingAlgorithm)
                        );
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
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(11), BigInteger.valueOf(21),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 9, 10, hashingAlgorithm),
                                range -> checkRange(range, 22, 66, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapRight() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(50), BigInteger.valueOf(3),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 9, 49, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapLeft() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(90), BigInteger.valueOf(15),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 16, 66, hashingAlgorithm)
                        );
            }

            @Test
            void computesDifferenceOverlapLeftAndRight() {
                assertThat(ringRange.computeDifference(new RingRangeImpl(BigInteger.valueOf(44), BigInteger.valueOf(21),
                        hashingAlgorithm)))
                        .satisfiesExactly(
                                range -> checkRange(range, 22, 43, hashingAlgorithm)
                        );
            }

        }

    }

}