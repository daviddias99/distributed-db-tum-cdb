package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;

import java.math.BigInteger;

/**
 * A hashing algorithm that hashes {@link String}s and {@link NetworkLocation}s.
 * The hashes are in the format of {@link BigInteger}s.
 *
 * @see BigInteger
 */
public interface HashingAlgorithm {

    /**
     * Prefix of hexadecimal strings
     */
    String HEX_PREFIX = "0x";

    /**
     * Converts a hash value into a hexadecimal string format without a prefix.
     *
     * @param hash the hash value to convert
     * @return the hexadecimal string representation
     */
    static String convertHashToHex(BigInteger hash) {
        return hash.toString(16);
    }

    /**
     * Converts a hash value into a hexadecimal string format with a "0x" prefix.
     *
     * @param hash the hash value to convert
     * @return the hexadecimal string representation
     */
    static String convertHashToHexWithPrefix(BigInteger hash) {
        return HEX_PREFIX + convertHashToHex(hash);
    }

    /**
     * Converts a hexadecimal string with or without a prefix into a hash value.
     *
     * @param hexString the hexadecimal string value to convert
     * @return the hash value representing that string
     */
    static BigInteger convertHexToHash(String hexString) {
        if (hexString.startsWith(HEX_PREFIX)) {
            hexString = hexString.substring(HEX_PREFIX.length());
        }
        return new BigInteger(hexString, 16);
    }

    /**
     * Pad given string with zeros on the left up to a given final string length
     *
     * @param inputString string to pad
     * @param length      final string length
     * @return padded string
     */
    static String padLeftZeros(String inputString, int length) {
        if (inputString.length() >= length) {
            return inputString;
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length - inputString.length()) {
            sb.append('0');
        }
        sb.append(inputString);

        return sb.toString();
    }

    /**
     * Returns the hash of the supplied {@link String} using the encoding from {@link Constants#TELNET_ENCODING} to
     * turn the {@link String} into {@code byte}s.
     *
     * @param string the {@link String} to hash
     * @return the hash value of the supplied {@link String}
     * @see ConsistentHashRing#getHashingAlgorithm()
     * @see Constants#TELNET_ENCODING
     */
    BigInteger hash(String string);

    /**
     * Returns the hash of the supplied {@link NetworkLocation}.
     * <p>
     * In the default implementation the {@link NetworkLocation} is turned the into a {@link String} of the format
     * {@code <address>:<port>} and then hashed using {@link #hash(String)}.
     *
     * @param networkLocation the {@link NetworkLocation} to hash
     * @return the hash value of the supplied {@link NetworkLocation}
     * @see #hash(String)
     */
    default BigInteger hash(NetworkLocation networkLocation) {
        return hash(String.format("%s:%s", networkLocation.getAddress(), networkLocation.getPort()));
    }

    /**
     * Returns the maximum value this {@link HashingAlgorithm} can map to.
     *
     * @return the maximum value this {@link HashingAlgorithm} can map to
     */
    BigInteger getMax();

    int getHashSizeBits();

}
