package de.tum.i13.server.kv;

/**
 * An {@link Exception} that signalizes that something went wrong in the key value store, either caused
 * by the user or the developer
 */
public class KVException extends Exception {

    /**
     * Creates a new {@link KVException} with the supplied message
     *
     * @param message the message of the exception
     */
    public KVException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link KVException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    public KVException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new {@link KVException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public KVException(String format, Object... args) {
        this(String.format(format, args));
    }

    /**
     * Creates a new {@link KVException} with the supplied format string and objects, and a cause
     *
     * @param format the string to format
     * @param args the parameters for the string formatting
     * @param cause the cause of the exception
     * @see String#format(String, Object...)
     */
    public KVException(Throwable cause, String format, Object... args) {
        this(String.format(format, args), cause);
    }

}
