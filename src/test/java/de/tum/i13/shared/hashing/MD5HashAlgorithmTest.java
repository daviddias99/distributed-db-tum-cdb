package de.tum.i13.shared.hashing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The correct hashes were generated using <a href="https://www.md5hashgenerator.com/">an online tool</a>.
 */
class MD5HashAlgorithmTest {

    private MD5HashAlgorithm algorithm;

    @BeforeEach
    void createAlgo() {
        algorithm = new MD5HashAlgorithm();
    }

    @Test
    void hashesLoremIpsum() {
        String loremIpsum = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor " +
                "invidunt ut labore et dolore magna aliquyam";
        assertThat(algorithm.hash(loremIpsum))
                .extracting(HashingAlgorithm::convertHashToHex)
                .isEqualTo("6f4d770bcbeb3c41bdb19524ee073c2b");
    }

    @Test
    void hashesFourStringsInARow() {
        assertThat(algorithm.hash("hello1"))
                .extracting(HashingAlgorithm::convertHashToHex)
                .isEqualTo("203ad5ffa1d7c650ad681fdff3965cd2");

        assertThat(algorithm.hash("hello2"))
                .extracting(HashingAlgorithm::convertHashToHex)
                .isEqualTo("6e809cbda0732ac4845916a59016f954");

        assertThat(algorithm.hash("hello3"))
                .extracting(HashingAlgorithm::convertHashToHex)
                .isEqualTo("7ce8be0fa3932e840f6a19c2b83e11ae");

        assertThat(algorithm.hash("hello4"))
                .extracting(HashingAlgorithm::convertHashToHex)
                .isEqualTo("a75f2192bae11cb76cdcdada9332bab6");
    }

}