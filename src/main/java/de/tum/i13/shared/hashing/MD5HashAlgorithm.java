package de.tum.i13.shared.hashing;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A {@link HashingAlgorithm} that implements the MD5 hashing algorithm using a {@link MessageDigest}.
 * The hashing algorithm MD5 might not be supported by every Java implementation as indicated in {@link MessageDigest}.
 *
 * @see MessageDigest
 */
public class MD5HashAlgorithm implements HashingAlgorithm {

    private static final Logger LOGGER = LogManager.getLogger(MD5HashAlgorithm.class);

    private final MessageDigest messageDigest;

    /**
     * Creates a new {@link MD5HashAlgorithm}
     *
     * @throws IllegalStateException if the MD5 hashing algorithm is not available
     */
    public MD5HashAlgorithm() {
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            final var exception = new IllegalStateException("Could not find MD5 hashing algorithm");
            LOGGER.fatal(Constants.THROWING_EXCEPTION_LOG_MESSAGE, exception);
            throw exception;
        }
    }

    @Override
    public BigInteger hash(String string) {
        LOGGER.info("Hashing the string '{}'", string);

        final byte[] bytes = string.getBytes(Constants.TELNET_ENCODING);
        final byte[] digest = messageDigest.digest(bytes);
        return new BigInteger(1, digest);
    }

    @Override
    public BigInteger getMax() {
        return Constants.MD5_HASH_MAX_VALUE;
    }

    @Override
    public String toString() {
        return "MD5HashAlgorithm";
    }

}
