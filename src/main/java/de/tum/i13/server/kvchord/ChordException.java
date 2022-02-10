package de.tum.i13.server.kvchord;

/**
 * An {@link Exception} that signalizes that something went wrong in the chord logic, either caused
 * by the user, developer or network
 */
public class ChordException extends Exception {

    /**
     * Creates a new {@link ChordException} with the supplied message
     *
     * @param message the message of the exception
     */
    public ChordException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link ChordException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause   the cause of the exception
     */
    public ChordException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new {@link ChordException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public ChordException(String format, Object... args) {
        this(String.format(format, args));
    }

    /**
     * Creates a new {@link ChordException} with the supplied format string and objects, and a cause
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param cause  the cause of the exception
     * @see String#format(String, Object...)
     */
    public ChordException(Throwable cause, String format, Object... args) {
        this(String.format(format, args), cause);
    }

}
