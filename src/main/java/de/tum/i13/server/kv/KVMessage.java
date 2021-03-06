package de.tum.i13.server.kv;

import java.util.Objects;
import java.util.function.Function;

/**
 * A message that is interchanged between a {@link KVStore} and a caller
 */
public interface KVMessage {

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
        final Function<String, IllegalArgumentException> exceptionFunction =
                receivedMessage -> new IllegalArgumentException(
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
            throw exceptionFunction.apply(message);
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
     * <p>
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

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
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
         * Put - request from server
         */
        PUT_SERVER(true, true),
        /**
         * Put - request from server
         */
        PUT_SERVER_OWNER(true, true),
        /**
         * Put - request successful, tuple inserted
         */
        PUT_SUCCESS(true, false),
        /**
         * Put - request successful, i.e. value updated
         */
        PUT_UPDATE(true, false),
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
         * Used by server to indicate start of shutdown
         */
        SERVER_SHUTDOWN(true, true),

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
        SERVER_NOT_RESPONSIBLE(false, false),

        /**
         * Used by server to indicate successful handoff
         */
        SERVER_HANDOFF_SUCCESS(false, false),

        SERVER_HANDOFF_ACK(false, false),

        /**
         * Used by server to ask broker for metadata
         */
        SERVER_START(true, true),

        /**
         * Generic acknowlegement from server
         */
        SERVER_ACK(false, false),

        /**
         * Indicates that the storage server is currently blocked for write requests due
         * to reallocation of data in case of joining or leaving storage nodes
         */
        SERVER_WRITE_LOCK(false, false),


        SERVER_WRITE_UNLOCK(false, false),

        /**
         * Message sent by client to request keyrange metadata (writting)
         */
        KEYRANGE(false, false),

        /**
         * Message sent by client to request keyrange metadata (reading)
         */
        KEYRANGE_READ(false, false),

        /**
         * Indicates the return of the key ranges and which KVStores are responsible for the ranges
         */
        SERVER_HEART_BEAT(false, false),

        /**
         * Used to indicate that the server is still alive. Usually an SERVER_HEART_BEAT
         * message is sent in response to ECS_HEART_BEAT
         */
        ECS_HEART_BEAT(false, false),

        /**
         * Signal a server to enter a write lock state
         */
        ECS_WRITE_LOCK(false, false),

        /**
         * Set server to allow writes
         */
        ECS_WRITE_UNLOCK(false, false),

        /**
         * ECS signals that handoff to peer (key) of elements (value) should start
         */
        ECS_HANDOFF(true, true),

        /**
         * Set server metadata
         */
        ECS_SET_KEYRANGE(true, false),

        ECS_ACK(false, false),

        ECS_WAITING_FOR_HANDOFF(false, false),

        /**
         * Indicates the return of the key ranges and which KVStores are responsible for
         * the ranges (writting)
         */
        KEYRANGE_SUCCESS(true, false),

        /**
         * Indicates the return of the key ranges and which KVStores are responsible for
         * the ranges (reading)
         */
        KEYRANGE_READ_SUCCESS(true, false),

        DELETE_SERVER(true, false),


        /* CHORD */

        CHORD_FIND_SUCCESSOR(true, false),

        CHORD_FIND_SUCESSSOR_RESPONSE(true, true),

        CHORD_CLOSEST_PRECEDING_FINGER(true, false),

        CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE(true, true),
        CHORD_GET_PREDECESSOR(false, false),
        CHORD_GET_PREDECESSOR_RESPONSE(true, false),
        CHORD_GET_SUCCESSORS(false, false),
        CHORD_GET_SUCCESSOR_RESPONSE(true, false),
        CHORD_NOTIFY(true, false),
        CHORD_NOTIFY_ACK(false, false),
        CHORD_GET_STATE_STR(false, false),
        CHORD_GET_STATE_STR_RESPONSE(true, false),
        CHORD_HEARTBEAT(false, false),
        CHORD_HEARTBEAT_RESPONSE(false, false);

        private final boolean needsKey;
        private final boolean needsValue;

        /**
         * Create a new status type
         *
         * @param needsKey   true if status is associated with a key
         * @param needsValue true if the status is associated with a value
         */
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

}
