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

import static de.tum.i13.TestUtils.checkRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;
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

    @Test
    void returnsAllLocations() {
        assertThat(hashRing.getAllNetworkLocations())
                .containsOnly(location1, location2, location3);
    }

    @Test
    void getsSize() {
        assertThat(hashRing.size())
                .isEqualTo(3);
    }

    @Nested
    class WithStubbedKeysTest {

        @BeforeEach
        void stubKeyHashes() {
            when(hashingAlgorithm.hash("key1"))
                    .thenReturn(BigInteger.valueOf(1));
            when(hashingAlgorithm.hash("key2"))
                    .thenReturn(BigInteger.valueOf(3));
            when(hashingAlgorithm.hash("key3"))
                    .thenReturn(BigInteger.valueOf(6));
        }

        @Test
        void returnsWriteResponsibleLocations() {
            assertThat(hashRing.getWriteResponsibleNetworkLocation("key1"))
                    .hasValue(location1);
            assertThat(hashRing.getWriteResponsibleNetworkLocation("key2"))
                    .hasValue(location2);
            assertThat(hashRing.getWriteResponsibleNetworkLocation("key3"))
                    .hasValue(location3);
        }

        @Test
        void returnsReadResponsibleLocations() {
            when(hashingAlgorithm.hash(location4))
                    .thenReturn(BigInteger.valueOf(10));
            hashRing.addNetworkLocation(location4);
            assumeThat(hashRing.isReplicationActive())
                    .isTrue();

            assertThat(hashRing.getReadResponsibleNetworkLocation("key1"))
                    .containsOnly(location1, location2, location3);
            assertThat(hashRing.getReadResponsibleNetworkLocation("key2"))
                    .containsOnly(location2, location3, location4);
            assertThat(hashRing.getReadResponsibleNetworkLocation("key3"))
                    .containsOnly(location3, location4, location1);
        }

        @Test
        void checksWriteResponsibility() {
            assertThat(hashRing.isWriteResponsible(location1, "key1"))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location1, "key2"))
                    .isFalse();
            assertThat(hashRing.isWriteResponsible(location1, "key3"))
                    .isFalse();

            assertThat(hashRing.isWriteResponsible(location1, "key1"))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location2, "key2"))
                    .isTrue();
            assertThat(hashRing.isWriteResponsible(location3, "key3"))
                    .isTrue();
        }

        @Test
        void checkReadResponsibility() {
            when(hashingAlgorithm.hash(location4))
                    .thenReturn(BigInteger.valueOf(10));
            hashRing.addNetworkLocation(location4);
            assumeThat(hashRing.isReplicationActive())
                    .isTrue();

            assertThat(hashRing.isReadResponsible(location1, "key1"))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location1, "key2"))
                    .isFalse();
            assertThat(hashRing.isReadResponsible(location1, "key3"))
                    .isTrue();

            assertThat(hashRing.isReadResponsible(location3, "key1"))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location4, "key2"))
                    .isTrue();
            assertThat(hashRing.isReadResponsible(location1, "key3"))
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
        checkRange(hashRing.getWriteRange(location1), 8, 2, hashingAlgorithm);
        checkRange(hashRing.getWriteRange(location2), 3, 4, hashingAlgorithm);
        checkRange(hashRing.getWriteRange(location3), 5, 7, hashingAlgorithm);
    }

    @Test
    void getsReplicatedReadRangesWithThreeLocations() {
        assumeThat(hashRing.isReplicationActive())
                .isTrue();

        checkRange(hashRing.getReadRange(location1), 3, 2, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location2), 5, 4, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), 8, 7, hashingAlgorithm);
    }

    @Test
    void getsReplicatedReadRangesWithFourLocations() {
        when(hashingAlgorithm.hash(location4))
                .thenReturn(BigInteger.valueOf(10));
        hashRing.addNetworkLocation(location4);
        assumeThat(hashRing.isReplicationActive())
                .isTrue();

        checkRange(hashRing.getReadRange(location1), 5, 2, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location2), 8, 4, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), 11, 7, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location4), 3, 10, hashingAlgorithm);
    }

    @Test
    void getsNotReplicatedReadRanges() {
        hashRing.removeNetworkLocation(location2);
        assumeThat(hashRing.isReplicationActive())
                .isFalse();

        checkRange(hashRing.getReadRange(location1), 8, 2, hashingAlgorithm);
        checkRange(hashRing.getReadRange(location3), 3, 7, hashingAlgorithm);
    }

    @Test
    void getsPrecedingNetworkLocations() {
        assertThat(hashRing.getPrecedingNetworkLocations(location1, 2))
                .containsExactly(location2, location3);
    }

    @Test
    void getsSucceedingNetworkLocations() {
        assertThat(hashRing.getPrecedingNetworkLocations(location2, 2))
                .containsExactly(location3, location1);
    }

    @Test
    void doesNotGetPredecessorsOnExceedingSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> hashRing.getPrecedingNetworkLocations(location1, 3));
    }

    @Test
    void doesNotGetSuccessorsOnExceedingSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> hashRing.getSucceedingNetworkLocations(location2, 3));
    }

    @Nested
    class WithLocationsStubbedTest {

        static final String WRITE_RANGES_REPRESENTATION = "8,2,location1:1;3,4,location2:2;5,7,location3:3;";

        @BeforeEach
        void stubLocations() {
            Map.of(location1, 1, location3, 3)
                    .forEach((location, number) -> {
                        when(location.getAddress()).thenReturn("location" + number);
                        when(location.getPort()).thenReturn(number);
                    });
        }

        @Test
        void packsReadRangesTwoLocations() {
            hashRing.removeNetworkLocation(location2);
            assumeThat(hashRing.isReplicationActive())
                    .isFalse();
            assumeThat(hashRing.packReadRanges())
                    .isEqualTo("8,2,location1:1;3,7,location3:3;");
        }

        @Nested
        class StubThirdLocationTest {

            @BeforeEach
            void stubLocation() {
                when(location2.getAddress()).thenReturn("location2");
                when(location2.getPort()).thenReturn(2);
            }

            @Test
            void packsWriteRanges() {
                assertThat(hashRing.packWriteRanges())
                        .isEqualTo(WRITE_RANGES_REPRESENTATION);
            }

            @Test
            void packsReadRangesThreeLocations() {
                assumeThat(hashRing.isReplicationActive())
                        .isTrue();
                assertThat(hashRing.packReadRanges())
                        .isEqualTo("3,2,location1:1;5,4,location2:2;8,7,location3:3;");
            }

            @Test
            void packsReadRangesFourLocations() {
                when(hashingAlgorithm.hash(location4))
                        .thenReturn(BigInteger.valueOf(10));
                when(location4.getAddress()).thenReturn("location4");
                when(location4.getPort()).thenReturn(4);
                hashRing.addNetworkLocation(location4);
                assumeThat(hashRing.isReplicationActive())
                        .isTrue();

                assertThat(hashRing.packReadRanges())
                        .isEqualTo("5,2,location1:1;8,4,location2:2;b,7,location3:3;3,a,location4:4;");
            }

            @Test
            void packsOnToString() {
                assertThat(hashRing)
                        .hasToString(WRITE_RANGES_REPRESENTATION);
            }

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

        @Test
        void getPredecessorsOnNotContainedLocation() {
            assertThat(hashRing.getPrecedingNetworkLocations(additionalLocation, 3))
                    .containsExactly(location2, location3, location1);
        }

        @Test
        void getSuccessorsOnNotContainedLocation() {
            assertThat(hashRing.getSucceedingNetworkLocations(additionalLocation, 3))
                    .containsExactly(location2, location3, location1);
        }

        @Test
        void doesNotGetPredecessorsOnExceedingSizeNotContainedLocation() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> hashRing.getPrecedingNetworkLocations(additionalLocation, 4));
        }

        @Test
        void doesNotGetSuccessorsOnExceedingSizeNotContainedLocation() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> hashRing.getSucceedingNetworkLocations(additionalLocation, 4));
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