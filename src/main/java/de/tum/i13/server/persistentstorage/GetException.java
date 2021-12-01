package de.tum.i13.server.persistentstorage;

import de.tum.i13.server.kv.KVException;

/**
 * A checked {@link Exception} thrown when getting a key from a {@link PersistentStorage} fails
 */
public class GetException extends KVException {

    /**
     * Creates a new {@link GetException} with the supplied message
     *
     * @param message the message of the exception
     */
    public GetException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link GetException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause   the cause of the exception
     */
    public GetException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new {@link GetException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public GetException(String format, Object... args) {
        super(format, args);
    }

    /**
     * Creates a new {@link GetException} with the supplied format string and objects, and a cause
     *
     * @param cause  the cause of the exception
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public GetException(Throwable cause, String format, Object... args) {
        super(cause, format, args);
    }

}
