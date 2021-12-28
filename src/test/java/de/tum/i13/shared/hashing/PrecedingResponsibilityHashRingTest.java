package de.tum.i13.shared.hashing;

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
        List.of(location1, location2)
                .forEach(hashRing::addNetworkLocation);
        hashRing.addNetworkLocation(BigInteger.valueOf(7), location3);
    }

    @Test
    void returnsThreeDifferentResponsibleLocations() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(1), BigInteger.valueOf(3), BigInteger.valueOf(6));

        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location1);
        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location2);
        assertThat(hashRing.getWriteResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location3);
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
            assertThat(hashRing.packMessage())
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