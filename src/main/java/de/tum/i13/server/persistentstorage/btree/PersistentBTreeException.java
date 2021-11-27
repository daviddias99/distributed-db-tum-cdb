package de.tum.i13.server.persistentstorage.btree;

/**
 * An {@link Exception} that signalizes that something went wrong in persistent
 * storage by the user or the developer
 */
public class PersistentBTreeException extends Exception {

    /**
     * Creates a new {@link PersistentBTreeException} with the supplied message
     *
     * @param message the message of the exception
     */
    public PersistentBTreeException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link PersistentBTreeException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause   the cause of the exception
     */
    public PersistentBTreeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new {@link PersistentBTreeException} with the supplied format string and
     * objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public PersistentBTreeException(String format, Object... args) {
        this(String.format(format, args));
    }

    /**
     * Creates a new {@link PersistentBTreeException} with the supplied format string and
     * objects, and a cause
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param cause  the cause of the exception
     * @see String#format(String, Object...)
     */
    public PersistentBTreeException(Throwable cause, String format, Object... args) {
        this(String.format(format, args), cause);
    }

}