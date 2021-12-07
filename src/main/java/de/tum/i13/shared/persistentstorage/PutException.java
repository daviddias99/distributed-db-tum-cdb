package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVException;

/**
 * A checked {@link Exception} thrown when putting a key with a value in a {@link PersistentStorage} fails
 */
public class PutException extends KVException {

    /**
     * Creates a new {@link PutException} with the supplied message
     *
     * @param message the message of the exception
     */
    public PutException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link PutException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause   the cause of the exception
     */
    public PutException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new {@link PutException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public PutException(String format, Object... args) {
        super(format, args);
    }

    /**
     * Creates a new {@link PutException} with the supplied format string and objects, and a cause
     *
     * @param cause  the cause of the exception
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public PutException(Throwable cause, String format, Object... args) {
        super(cause, format, args);
    }

}
