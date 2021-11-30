package de.tum.i13.server.kv;

import java.util.Objects;
import java.util.function.Function;

/**
 * A message that is interchanged between a {@link KVStore} and a caller
 */
public interface KVMessage {

    /**
     * The type and status of the {@link KVMessage}
     */
    enum StatusType {
        /**
         * Error status, for example unprocessable command
         */
        ERROR(false, false),
        /**
         * Get - request
         */
        GET(true, false),
        /**
         * requested tuple (i.e. value) not found
         */
        GET_ERROR(true, false),
        /**
         * requested tuple (i.e. value) found
         */
        GET_SUCCESS(true, true),
        /**
         * Put - request
         */
        PUT(true, true),
        /**
         * Put - request successful, tuple inserted
         */
        PUT_SUCCESS(true, true),
        /**
         * Put - request successful, i.e. value updated
         */
        PUT_UPDATE(true, true),
        /**
         * Put - request not successful
         */
        PUT_ERROR(true, true),
        /**
         * Delete - request
         */
        DELETE(true, false),
        /**
         * Delete - request successful
         */
        DELETE_SUCCESS(true, false),
        /**
         * Delete - request successful
         */
        DELETE_ERROR(true, false),
        /**
         * Indicates that currently no requests are processed by the server since the
         * whole storage service is under initialization.
         * Retries with exponential back-off with jitter should be used.
         */
        SERVER_STOPPED(false, false),
        /**
         * Indicates that the requested key is not within the range of the answering
         * server
         */
        SERVER_NOT_RESPONSIBLE(true, false),

        /**
         * Signal a server to enter a write lock state
         */
        ECS_WRITE_LOCK(false, false),

        /**
         * Signal a server to enter a write lock state
         */
        ECS_KEYRANGE(true, false),

        /**
         * Indicates that the storage server is currently blocked for write requests due
         * to reallocation of data in case of joining or leaving storage nodes
         */
        SERVER_WRITE_LOCK(false, false),
        /**
         * Indicates the return of the key ranges and which KVStores are responsible for
         * the ranges
         */
        KEYRANGE_SUCCESS(true, false),
        /**
         * Used to indicate that the server is still alive. Usually an HEART_BEAT
         * message is sent in response to another HEART_BEAT
         */
        HEART_BEAT(false, false);

        private final boolean needsKey;
        private final boolean needsValue;

        StatusType(boolean needsKey, boolean needsValue) {
            this.needsKey = needsKey;
            this.needsValue = needsValue;
        }

        /**
         * Check if the {@link KVMessage} with this {@link StatusType} needs a non-empty
         * key
         *
         * @return if it needs a non-empty key
         */
        public boolean needsKey() {
            return needsKey;
        }

        /**
         * Check if the {@link KVMessage} with this {@link StatusType} needs a non-empty
         * value
         *
         * @return if it needs a non-empty value
         */
        public boolean needsValue() {
            return this.needsValue;
        }
    }

    /**
     * @return the key that is associated with this message,
     *         null if not key is associated.
     */
    String getKey();

    /**
     * @return the value that is associated with this message,
     *         null if not value is associated.
     */
    String getValue();

    /**
     * @return a status string that is used to identify request types,
     *         response types and error types associated to the message.
     */
    StatusType getStatus();

    /**
     * Packs the message into a {@link String} format.
     *
     * @return the message encoded as a {@link String}
     */
    default String packMessage() {
        final StatusType status = getStatus();
        return String.format(
                "%s %s %s",
                status.toString().toLowerCase(),
                Objects.toString(getKey(), ""),
                Objects.toString(getValue(), "")).trim();
    }

    /**
     * Unpacks a message in the {@link String} format using the standardc
     * implementation {@link KVMessageImpl}.
     * Uses {@link #extractTokens(String)} to extract the tokens from the message.
     * Refer to the specification document for further details.
     *
     * @param message the message encoded as a {@link String}, must not be empty and
     *                adhere to one of the message
     *                format from the specification
     * @return the message converted to a {@link KVMessage}
     * @throws IllegalArgumentException if the message could not be converted
     *                                  according to the specification
     * @see KVMessageImpl
     * @see #extractTokens(String)
     */
    static KVMessage unpackMessage(String message) {
        String[] msgTokens = extractTokens(message);

        final Function<String, StatusType> stringToStatusType = (String string) -> StatusType.valueOf(
                string.toUpperCase());
        final Function<String, IllegalArgumentException> exceptionFunction = receivedMessage -> new IllegalArgumentException(
                String.format(
                        "Could not convert \"%s\" to a %s",
                        receivedMessage,
                        KVMessage.class.getSimpleName()));

        if (msgTokens.length == 1) {
            if ("".equals(msgTokens[0])) {
                throw exceptionFunction.apply(message);
            } else {
                return new KVMessageImpl(stringToStatusType.apply(msgTokens[0]));
            }
        } else if (msgTokens.length == 2) {
            return new KVMessageImpl(msgTokens[1], stringToStatusType.apply(msgTokens[0]));
        } else if (msgTokens.length == 3) {
            return new KVMessageImpl(
                    msgTokens[1],
                    msgTokens[2],
                    stringToStatusType.apply(msgTokens[0]));
        } else {
            throw exceptionFunction.apply("message");
        }
    }

    /**
     * Trims the message and then extracts the tokens of the message.
     * Multiple spaces between tokens are considered as one and will not be part of
     * the token.
     * Keys cannot contain spaces, but values can.
     * I.e.
     * 
     * <pre>
     * "    put   thisKey     to this   value   "
     * </pre>
     * 
     * will produce the following tokens
     * 
     * <pre>
     * { "put", "thisKey", "to this   value" }
     * </pre>
     *
     * @param message the message from which to extract the tokens
     * @return the extracted tokens of the message
     * @see String#trim()
     */
    static String[] extractTokens(String message) {
        return message.trim().split("\\s+", 3);
    }

}
