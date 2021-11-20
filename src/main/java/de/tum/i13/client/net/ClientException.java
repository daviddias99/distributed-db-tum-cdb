package de.tum.i13.client.net;

public class ClientException extends Exception {

    private final String reason;
    private final Type type;

    // TODO Adapt to use proper cause and message
    public ClientException(String reason) {
        this.reason = reason;
        this.type = Type.UNKNOWN_ERROR;
    }

    public ClientException(String reason, Type type) {
        this.reason = reason;
        this.type = type;
    }

    public Type getType() {
        return this.type;
    }

    public String getReason() {
        return this.reason;
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
