package de.tum.i13.client.net;

/**
 * An {@link Exception} that signalizes that something went wrong in the network component of the client.
 */
public class ClientException extends Exception {

    private final Type type;

    /**
     * Creates a new {@link ClientException} with the supplied message, a cause and a type
     *
     * @param cause   the cause of the exception
     * @param type    the {@link Type} of the exception
     * @param message the message of the exception
     */
    public ClientException(Throwable cause, Type type, String message) {
        super(message, cause);
        this.type = type;
    }

    /**
     * Creates a new {@link ClientException} with the supplied message and a cause
     *
     * @param cause   the cause of the exception
     * @param message the message of the exceptino
     */
    public ClientException(Throwable cause, String message) {
        this(cause, Type.UNKNOWN_ERROR, message);
    }

    /**
     * Creates a new {@link ClientException} with the supplied message and type
     *
     * @param type    the {@link Type} of the exception
     * @param message the message of the exception
     */
    public ClientException(Type type, String message) {
        this(null, type, message);
    }

    /**
     * Creates a new {@link ClientException} with the supplied message
     *
     * @param message the message of the exception
     */
    public ClientException(String message) {
        this((Throwable) null, message);
    }

    /**
     * Creates a new {@link ClientException} with the supplied format string and objects, a cause and a type
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param cause  the cause of the exception
     * @param type   the {@link Type} of the exception
     * @see String#format(String, Object...)
     */
    public ClientException(Throwable cause, Type type, String format, Object... args) {
        this(cause, type, String.format(format, args));
    }

    /**
     * Creates a new {@link ClientException} with the supplied format string and objects, and a cause
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param cause  the cause of the exception
     * @see String#format(String, Object...)
     */
    public ClientException(Throwable cause, String format, Object... args) {
        this(cause, String.format(format, args));
    }

    /**
     * Creates a new {@link ClientException} with the supplied format string and objects, and a type
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @param type   the {@link Type} of the exception
     * @see String#format(String, Object...)
     */
    public ClientException(Type type, String format, Object... args) {
        this(type, String.format(format, args));
    }

    /**
     * Creates a new {@link ClientException} with the supplied format string and objects
     *
     * @param format the string to format
     * @param args   the parameters for the string formatting
     * @see String#format(String, Object...)
     */
    public ClientException(String format, Object... args) {
        this((Throwable) null, String.format(format, args));
    }

    /**
     * Gets the type of the exception
     *
     * @return the type of the exception
     */
    public Type getType() {
        return this.type;
    }

    /**
     * The type of the situation that caused the exception to be thrown.
     */
    public enum Type {
        /**
         * An error occurred while connecting, i.e. creating the socket or connecting to its input and output stream
         */
        CONNECTION_ERROR,
        /**
         * An error occurred while closing the socket
         */
        SOCKET_CLOSING_ERROR,
        /**
         * An error occurred while resolving the host name address
         */
        UNKNOWN_HOST,
        /**
         * An error occurred because the message to send was too large
         */
        MESSAGE_TOO_LARGE,
        /**
         * An error occurred because of an internal problem
         */
        INTERNAL_ERROR,
        /**
         * An error occurred because the client was not connected
         */
        UNCONNECTED,
        /**
         * An error occurred because of a not further specified reason
         */
        UNKNOWN_ERROR,
    }

}
