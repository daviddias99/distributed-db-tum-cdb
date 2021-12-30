package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class PrecedingResponsibilityHashRingTest {

    static final String IGNORED_STRING = "ignoredString";
    @Mock
    HashingAlgorithm hashingAlgorithm;
    PrecedingResponsibilityHashRing hashRing;
    @Mock(name = "Location 1")
    NetworkLocation location1;
    @Mock(name = "Location 2")
    NetworkLocation location2;
    @Mock(name = "Location 3")
    NetworkLocation location3;

    @Mock(name = "Location 4")
    NetworkLocation location4;

    @BeforeEach
    void setupHashRing() {
        // Add three network locations with hashes 2, 4, and 7
        final NavigableMap<BigInteger, NetworkLocation> networkLocationMap = new TreeMap<>();
        hashRing = mock(PrecedingResponsibilityHashRing.class,
                withSettings()
                        .useConstructor(hashingAlgorithm, networkLocationMap)
                        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
        );

        assertThat(hashRing.getHashingAlgorithm())
                .isEqualTo(hashingAlgorithm);

        when(hashingAlgorithm.hash(location1))
                .thenReturn(BigInteger.valueOf(2));
        when(hashingAlgorithm.hash(location2))
                .thenReturn(BigInteger.valueOf(4));
        when(hashingAlgorithm.hash(location3))
                .thenReturn(BigInteger.valueOf(7));
        List.of(location1, location2, location3)
                .forEach(hashRing::addNetworkLocation);

    }

    @Test
    void addsLocationDirectlyViaHash() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(9));

        hashRing.addNetworkLocation(BigInteger.valueOf(10), location4);
        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location4);
    }

    @Nested
    class WithStubbedKeysTest {

        @BeforeEach
        void stubKeyHashes() {
            when(hashingAlgorithm.hash(anyString()))
                    .thenReturn(BigInteger.valueOf(1), BigInteger.valueOf(3), BigInteger.valueOf(6),
                            BigInteger.valueOf(1), BigInteger.valueOf(3), BigInteger.valueOf(6));
        }

        @Test
        void returnsThreeDifferentResponsibleLocations() {
            assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                    .hasValue(location1);
            assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                    .hasValue(location2);
            assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                    .hasValue(location3);
        }

        // TODO get read responsiblity test

        @Test
        void checksWriteResponsibility() {
            assertThat(hashRing.isWriteResponsible(location1, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location1, IGNORED_STRING))
                    .isFalse();
            assertThat(hashRing.isWriteResponsible(location1, IGNORED_STRING))
                    .isFalse();

            assertThat(hashRing.isWriteResponsible(location1, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location2, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location3, IGNORED_STRING))
                    .isTrue();
        }

        @Test
        void checkReadResponsibility() {
            when(hashingAlgorithm.hash(location4))
                    .thenReturn(BigInteger.valueOf(10));
            hashRing.addNetworkLocation(location4);
            assumeThat(hashRing.isReplicationActive())
                    .isTrue();

            assertThat(hashRing.isReadResponsible(location1, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location1, IGNORED_STRING))
                    .isFalse();
            assertThat(hashRing.isReadResponsible(location1, IGNORED_STRING))
                    .isTrue();

            assertThat(hashRing.isReadResponsible(location3, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location4, IGNORED_STRING))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location1, IGNORED_STRING))
                    .isTrue();
        }

    }


    @Test
    void wrapsResponsibilityAround() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(6));
        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location3);
    }

    @Test
    void returnsResponsibilityWithHashSameAsServer() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(2));
        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location1);
    }

    @Test
    void replicatesWithEnoughLocations() {
        assumeThat(Constants.NUMBER_OF_REPLICAS)
                .isEqualTo(2);

        assertThat(hashRing.isReplicationActive())
                .isTrue();
    }

    @Test
    void doesNotReplicateWithNotEnoughLocations() {
        assumeThat(Constants.NUMBER_OF_REPLICAS)
                .isEqualTo(2);

        hashRing.removeNetworkLocation(location1);
        assertThat(hashRing.isReplicationActive())
                .isFalse();
    }

    @Test
    void getsWriteRanges() {
        checkRange(hashRing.getWriteRange(location1), BigInteger.valueOf(8), BigInteger.valueOf(2), hashingAlgorithm);
        checkRange(hashRing.getWriteRange(location2), BigInteger.valueOf(3), BigInteger.valueOf(4), hashingAlgorithm);
        checkRange(hashRing.getWriteRange(location3), BigInteger.valueOf(5), BigInteger.valueOf(7), hashingAlgorithm);
    }

    @Test
    void getsReplicatedReadRangesWithThreeLocations() {
        assumeThat(hashRing.isReplicationActive())
                .isTrue();

        checkRange(hashRing.getReadRange(location1), BigInteger.valueOf(3), BigInteger.valueOf(2), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location2), BigInteger.valueOf(5), BigInteger.valueOf(4), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), BigInteger.valueOf(8), BigInteger.valueOf(7), hashingAlgorithm);
    }

    @Test
    void getsReplicatedReadRangesWithFourLocations() {
        when(hashingAlgorithm.hash(location4))
                .thenReturn(BigInteger.valueOf(10));
        hashRing.addNetworkLocation(location4);
        assumeThat(hashRing.isReplicationActive())
                .isTrue();

        checkRange(hashRing.getReadRange(location1), BigInteger.valueOf(5), BigInteger.valueOf(2), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location2), BigInteger.valueOf(8), BigInteger.valueOf(4), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), BigInteger.valueOf(11), BigInteger.valueOf(7), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location4), BigInteger.valueOf(3), BigInteger.valueOf(10), hashingAlgorithm);
    }

    @Test
    void getsNotReplicatedReadRanges() {
        hashRing.removeNetworkLocation(location2);
        assumeThat(hashRing.isReplicationActive())
                .isFalse();

        checkRange(hashRing.getReadRange(location1), BigInteger.valueOf(8), BigInteger.valueOf(2), hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), BigInteger.valueOf(3), BigInteger.valueOf(7), hashingAlgorithm);
    }

    static void checkRange(RingRange ringRange, BigInteger start, BigInteger end, HashingAlgorithm hashingAlgorithm) {
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

    @Nested
    class WithLocationsStubbedTest {

        static final String STRING_REPRESENTATION = "8,2,location1:1;3,4,location2:2;5,7,location3:3;";

        @BeforeEach
        void stubLocations() {
            Map.of(location1, 1, location2, 2, location3, 3)
                    .forEach((location, number) -> {
                        when(location.getAddress()).thenReturn("location" + number);
                        when(location.getPort()).thenReturn(number);
                    });
        }

        @Test
        void packsMetadata() {
            assertThat(hashRing.packWriteRanges())
                    .isEqualTo(STRING_REPRESENTATION);
        }

        @Test
        void packsOnToString() {
            assertThat(hashRing)
                    .hasToString(STRING_REPRESENTATION);
        }

    }

    @Nested
    class WithAdditionalLocationTest {

        @Mock(name = "My network location")
        NetworkLocation additionalLocation;

        @BeforeEach
        void stubAdditionalLocation() {
            when(hashingAlgorithm.hash(additionalLocation))
                    .thenReturn(BigInteger.valueOf(3));
        }

        @Test
        void getsPrecedingNetworkLocation() {
            assertThat(hashRing.getPrecedingNetworkLocation(additionalLocation))
                    .hasValue(location1);
        }

        @Test
        void getsSucceedingNetworkLocation() {
            assertThat(hashRing.getSucceedingNetworkLocation(additionalLocation))
                    .hasValue(location2);
        }

        @Test
        void doesNotContainUnknownLocation() {
            assertThat(hashRing.contains(additionalLocation))
                    .isFalse();
        }

    }

    @Test
    void containsKnownLocation() {
        assertThat(hashRing.contains(location1))
                .isTrue();
    }

    @Test
    void removesLocation() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(3));

        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .get()
                .isEqualTo(location2);

        hashRing.removeNetworkLocation(location2);

        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .get()
                .isEqualTo(location3);
    }



}