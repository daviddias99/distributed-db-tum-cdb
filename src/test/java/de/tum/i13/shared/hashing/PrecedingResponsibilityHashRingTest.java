package de.tum.i13.shared.hashing;

import de.tum.i13.shared.NetworkLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class PrecedingResponsibilityHashRingTest {

    private static final String IGNORED_STRING = "ignoredString";
    private static final String STRING_REPRESENTATION = "8,2,location1:1;3,4,location2:2;5,7,location3:3;";
    public static final String NEEDS_LOCATION_STUBBING = "needs_location_stubbing";
    @Mock
    private HashingAlgorithm hashingAlgorithm;
    private PrecedingResponsibilityHashRing hashRing;
    @Mock(name = "Location 1")
    private NetworkLocation location1;
    @Mock(name = "Location 2")
    private NetworkLocation location2;
    @Mock(name = "Location 3")
    private NetworkLocation location3;

    @BeforeEach
    void setupHashRing(TestInfo testInfo) {
        if (testInfo.getTags().contains(NEEDS_LOCATION_STUBBING)) {
            Map.of(location1, 1, location2, 2, location3, 3)
                    .forEach((location, number) -> {
                        when(location.getAddress()).thenReturn("location" + number);
                        when(location.getPort()).thenReturn(number);
                    });
        }

        // Add three network locations with hashes 2, 4, and 7
        final NavigableMap<BigInteger, NetworkLocation> networkLocationMap = new TreeMap<>();
        hashRing = mock(PrecedingResponsibilityHashRing.class,
                withSettings()
                        .useConstructor(hashingAlgorithm, networkLocationMap)
                        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
        );

        assertThat(hashRing.getHashingAlgorithm())
                .isEqualTo(hashingAlgorithm);

        when(hashingAlgorithm.hash(any(NetworkLocation.class)))
                .thenReturn(BigInteger.valueOf(2), BigInteger.valueOf(4));
        List.of(location1, location2)
                .forEach(hashRing::addNetworkLocation);
        hashRing.addNetworkLocation(BigInteger.valueOf(7), location3);
    }

    @Test
    void returnsThreeDifferentResponsibleLocations() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(1), BigInteger.valueOf(3), BigInteger.valueOf(6));

        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location1);
        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location2);
        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location3);
    }

    @Test
    void wrapsResponsibilityAround() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(6));
        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location3);
    }

    @Test
    void returnsResponsibilityWithHashSameAsServer() {
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(2));
        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .hasValue(location1);
    }

    @Test
    @Tag(NEEDS_LOCATION_STUBBING)
    void packsMetadata() {
        assertThat(hashRing.packMessage())
                .isEqualTo(STRING_REPRESENTATION);
    }

    @Test
    @Tag(NEEDS_LOCATION_STUBBING)
    void packsOnToString() {
        assertThat(hashRing)
                .hasToString(STRING_REPRESENTATION);
    }

    @Test
    void removesLocation(@Mock(name = "My network location") NetworkLocation location) {
        when(hashingAlgorithm.hash(location))
                .thenReturn(BigInteger.valueOf(4));
        when(hashingAlgorithm.hash(anyString()))
                .thenReturn(BigInteger.valueOf(3));

        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .get()
                .isEqualTo(location2);

        hashRing.removeNetworkLocation(location);

        assertThat(hashRing.getResponsibleNetworkLocation(IGNORED_STRING))
                .get()
                .isEqualTo(location3);
    }

}