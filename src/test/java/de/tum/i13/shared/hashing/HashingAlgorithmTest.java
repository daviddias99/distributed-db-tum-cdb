package de.tum.i13.shared.hashing;

import de.tum.i13.shared.NetworkLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;

import static de.tum.i13.shared.hashing.HashingAlgorithm.convertHashToHex;
import static de.tum.i13.shared.hashing.HashingAlgorithm.convertHashToHexWithPrefix;
import static java.math.BigInteger.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HashingAlgorithmTest {

    @Test
    void convertsHashToHex() {
        assertThat(convertHashToHex(valueOf(851937882)))
                .isEqualTo("32c78a5a");
    }

    @Test
    void convertsHashToHexWithPrefix() {
        assertThat(convertHashToHexWithPrefix(valueOf(619971785)))
                .isEqualTo(HashingAlgorithm.HEX_PREFIX + "24f404c9");
    }

    @Test
    void delegatesLocationHashing(@Mock HashingAlgorithm algorithm, @Mock NetworkLocation location) {
        final BigInteger hash = valueOf(12345);
        when(location.getAddress())
                .thenReturn("myLocation");
        when(location.getPort())
                .thenReturn(42);
        when(algorithm.hash(location))
                .thenCallRealMethod();
        when(algorithm.hash("myLocation:42"))
                .thenReturn(hash);

        assertThat(algorithm.hash(location))
                .isEqualTo(hash);
        verify(algorithm)
                .hash("myLocation:42");
    }

}