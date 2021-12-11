package de.tum.i13.shared.hashing;

import org.junit.jupiter.api.Test;

import de.tum.i13.shared.net.NetworkLocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ConsistentHashRingTest {

    @Test
    void unpacksMetadata() {
        final String metadata = "aafac43d705f8629b79fa156eca28d34,044c13f702d774f2064033cbf6a2fa86,location1:42;" +
                "044c13f702d774f2064033cbf6a2fa87,613c6f13dc2a9b41d41a6943ce541586,location2:3;" +
                "613c6f13dc2a9b41d41a6943ce541587,98affbf5bcb4143dfd53ea9654a5a9a8,location3:4;" +
                "98affbf5bcb4143dfd53ea9654a5a9a9,aafac43d705f8629b79fa156eca28d33,location4:15;";
        ConsistentHashRing hashRing = ConsistentHashRing.unpackMetadata(metadata);

        assertThat(hashRing)
                .isInstanceOf(TreeMapServerMetadata.class)
                .extracting(ConsistentHashRing::getHashingAlgorithm)
                .isInstanceOf(MD5HashAlgorithm.class);

        // MD5 hash of "qwerty" is d8578edf8458ce06fbc5bb76a58c5ca4
        assertThat(hashRing.getResponsibleNetworkLocation("qwerty"))
                .get()
                .extracting(NetworkLocation::getAddress, NetworkLocation::getPort)
                .containsExactly("location1", 42);

        // MD5 hash of "hello" is 5d41402abc4b2a76b9719d911017c592
        assertThat(hashRing.getResponsibleNetworkLocation("hello"))
                .get()
                .extracting(NetworkLocation::getAddress, NetworkLocation::getPort)
                .containsExactly("location2", 3);

        // MD5 hash of "asdf" is 912ec803b2ce49e4a541068d495ab570
        assertThat(hashRing.getResponsibleNetworkLocation("asdf"))
                .get()
                .extracting(NetworkLocation::getAddress, NetworkLocation::getPort)
                .containsExactly("location3", 4);

        // MD5 hash of "4" is a87ff679a2f3e71d9181a67b7542122c
        assertThat(hashRing.getResponsibleNetworkLocation("4"))
                .get()
                .extracting(NetworkLocation::getAddress, NetworkLocation::getPort)
                .containsExactly("location4", 15);
    }

    @Test
    void failsOnTooMuchKVStoreData() {
        final String metadata = "1,2,location:4,2;" +
                "3,4,2,location2:2;";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ConsistentHashRing.unpackMetadata(metadata))
                .withMessageContainingAll("Could not convert", "malformed");
    }

    @Test
    void failsOnTooMuchLocationData() {
        final String metadata = "5,2,location:4:6;" +
                "3,4,location2:2:13;";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ConsistentHashRing.unpackMetadata(metadata))
                .withMessageContainingAll("Could not convert", "malformed");
    }

}