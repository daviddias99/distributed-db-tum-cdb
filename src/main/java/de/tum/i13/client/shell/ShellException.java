package de.tum.i13.client.shell;

/**
 * An unchecked checked {@link Exception} thrown when the shell encounters unexpected behavior
 */
class ShellException extends RuntimeException {

    /**
     * Creates a new {@link ShellException} with the supplied message
     *
     * @param message the message of the exception
     */
    ShellException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link ShellException} with the supplied message and cause
     *
     * @param message the message of the exception
     * @param cause   the cause of the exception
     */
    ShellException(String message, Throwable cause) {
        super(message, cause);
    }


    /**
     * Creates a new {@link ShellException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    ShellException(String format, Object... args) {
        this(String.format(format, args));
    }

    /**
     * Creates a new {@link ShellException} with the supplied format string and objects, and a cause
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param cause  the cause of the exception
     * @see String#format(String, Object...)
     */
    ShellException(Throwable cause, String format, Object... args) {
        this(String.format(format, args), cause);
    }

}