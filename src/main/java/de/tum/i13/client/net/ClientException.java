package de.tum.i13.client.net;

public class ClientException extends Exception {

    private final Type type;

    public ClientException(Throwable cause, Type type, String message) {
        super(message, cause);
        this.type = type;
    }

    public ClientException(Throwable cause, String message) {
        this(cause, Type.UNKNOWN_ERROR, message);
    }

    public ClientException(Type type, String message) {
        this(null, type, message);
    }

    public ClientException(String message) {
        this((Throwable) null, message);
    }

    public ClientException(Throwable cause, Type type, String format, Object... args) {
        this(cause, type, String.format(format, args));
    }

    public ClientException(Throwable cause, String format, Object... args) {
        this(cause, String.format(format, args));
    }

    public ClientException(Type type, String format, Object... args) {
        this(type, String.format(format, args));
    }

    public ClientException(String format, Object... args) {
        this((Throwable) null, String.format(format, args));
    }

    public Type getType() {
        return this.type;
    }

    public enum Type {
        SOCKET_CREATION_ERROR,
        SOCKET_CLOSING_ERROR,
        SOCKET_OPENING_ERROR,
        UNKNOWN_HOST,
        ERROR_CONNECTING,
        MESSAGE_TOO_LARGE,
        INTERNAL_ERROR,
        UNCONNECTED,
        UNKNOWN_ERROR,
    }

}
