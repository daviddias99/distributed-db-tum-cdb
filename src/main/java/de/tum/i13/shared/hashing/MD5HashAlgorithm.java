package de.tum.i13.shared.hashing;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * A {@link HashingAlgorithm} that implements the MD5 hashing algorithm using a {@link MessageDigest}.
 * The hashing algorithm MD5 might not be supported by every Java implementation as indicated in {@link MessageDigest}.
 *
 * @see MessageDigest
 */
public class MD5HashAlgorithm implements HashingAlgorithm {

    private static final Logger LOGGER = LogManager.getLogger(MD5HashAlgorithm.class);

    private final HashFunction hashFunction;
    private final BigInteger max;

    /**
     * Creates a new {@link MD5HashAlgorithm}
     *
     * @throws IllegalStateException if the MD5 hashing algorithm is not available
     */
    @SuppressWarnings("deprecation")
    public MD5HashAlgorithm() {
        hashFunction = Hashing.md5();
        max = BigInteger.TWO
                .pow(hashFunction.bits())
                .subtract(BigInteger.ONE);
    }

    @Override
    public BigInteger hash(String string) {
        LOGGER.trace("Hashing the string '{}'", string);

        final HashCode hashCode = hashFunction.hashString(string, Constants.TELNET_ENCODING);
        return new BigInteger(1, hashCode.asBytes());
    }

    @Override
    public BigInteger getMax() {
        return max;
    }

    @Override
    public String toString() {
        return "MD5HashAlgorithm";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MD5HashAlgorithm;
    }

    @Override
    public int hashCode() {
        return Objects.hash();
    }

    @Override
    public int getHashSizeBits() {
        return 128;
    }

}
