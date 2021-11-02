package de.tum.i13.client.exceptions;

public enum ClientExceptionType {
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
