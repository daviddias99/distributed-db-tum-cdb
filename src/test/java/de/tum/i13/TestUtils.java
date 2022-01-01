package de.tum.i13;

import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.RingRange;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;


public class TestUtils {

    private TestUtils() {

    }

    public static void checkRange(RingRange ringRange, BigInteger start, BigInteger end,
                                  HashingAlgorithm hashingAlgorithm) {
        assertThat(ringRange)
                .as("Supplied range must not be null")
                .isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(ringRange.getStart())
                    .as("Checking ring range start")
                    .isEqualTo(start);
            softly.assertThat(ringRange.getEnd())
                    .as("Checking ring range end")
                    .isEqualTo(end);
            softly.assertThat(ringRange.getHashingAlgorithm())
                    .as("Checking hashing algorithm")
                    .isEqualTo(hashingAlgorithm);
        });
    }

    public static void checkRange(RingRange ringRange, int start, int end, HashingAlgorithm hashingAlgorithm) {
        checkRange(ringRange, BigInteger.valueOf(start), BigInteger.valueOf(end), hashingAlgorithm);
    }

}
