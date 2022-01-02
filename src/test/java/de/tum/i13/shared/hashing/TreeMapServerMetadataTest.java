package de.tum.i13.shared.hashing;

import de.tum.i13.shared.net.NetworkLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TreeMapServerMetadataTest {

    PrecedingResponsibilityHashRing hashRing;
    @Mock(name = "Location 1")
    NetworkLocation location1;
    @Mock(name = "Location 2")
    NetworkLocation location2;
    @Mock(name = "Location 3")
    NetworkLocation location3;

    @BeforeEach
    void setupHashRing() {
        // Add three network locations
        hashRing = new TreeMapServerMetadata();

        when(location1.getAddress())
                .thenReturn("location1");
        when(location2.getAddress())
                .thenReturn("location2");
        when(location3.getAddress())
                .thenReturn("location3");
        List.of(location1, location2, location3)
                .forEach(hashRing::addNetworkLocation);

    }

    @Test
    void copiesHashRing() {
        final ConsistentHashRing copy = hashRing.copy();
        assertThat(copy)
                .isEqualTo(hashRing);
        copy.removeNetworkLocation(location1);
        assertThat(copy)
                .isNotEqualTo(hashRing);
    }

}