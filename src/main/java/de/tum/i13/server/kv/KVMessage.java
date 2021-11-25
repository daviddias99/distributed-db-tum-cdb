package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;

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
         * Undefined status, for example unprocessable command
         */
        UNDEFINED(false),
        /**
         * Get - request
         */
        GET(false),
        /**
         * requested tuple (i.e. value) not found
         */
        GET_ERROR(false),
        /**
         * requested tuple (i.e. value) found
         */
        GET_SUCCESS(true),
        /**
         * Put - request
         */
        PUT(true),
        /**
         * Put - request successful, tuple inserted
         */
        PUT_SUCCESS(true),
        /**
         * Put - request successful, i.e. value updated
         */
        PUT_UPDATE(true),
        /**
         * Put - request not successful
         */
        PUT_ERROR(true),
        /**
         * Delete - request
         */
        DELETE(false),
        /**
         * Delete - request successful
         */
        DELETE_SUCCESS(false),
        /**
         * Delete - request successful
         */
        DELETE_ERROR(false);

        private final boolean needsValue;

        StatusType(boolean needsValue) {
            this.needsValue = needsValue;
        }

        /**
         * Check if the {@link KVMessage} with this {@link StatusType} needs a non-empty value
         *
         * @return if it needs a non-empty value
         */
        public boolean needsValue() {
            return this.needsValue;
        }
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
                status == StatusType.UNDEFINED ? "error undefined error" : status.toString().toLowerCase(),
                Objects.toString(getKey(), ""),
                Objects.toString(getValue(), "")
        ).trim();
    }

    /**
     * Unpacks a message in the {@link String} format using the standard implementation {@link KVMessageImpl}.
     * Uses {@link #extractTokens(String)} to extract the tokens from the message.
     * Refer to the specification document for further details.
     *
     * @param message the message encoded as a {@link String}
     * @return the message converted to a {@link KVMessage}
     * @throws IllegalArgumentException if the message could not be converted according to the specification
     * @see KVMessageImpl
     * @see #extractTokens(String)
     */
    static KVMessage unpackMessage(String message) {
        String[] msgTokens = extractTokens(message);

        final Function<String, StatusType> stringToStatusType = (String string) -> StatusType.valueOf(
                string.toUpperCase()
        );
        if (msgTokens.length == 2) {
            return new KVMessageImpl(msgTokens[1], stringToStatusType.apply(msgTokens[0]));
        } else if (msgTokens.length == 3) {
            return new KVMessageImpl(
                    msgTokens[1],
                    msgTokens[2],
                    stringToStatusType.apply(msgTokens[0]));
        } else {
            throw new IllegalArgumentException(String.format(
                    "Could not convert \"%s\" to a %s",
                    message,
                    KVMessage.class.getSimpleName()
            ));
        }
    }

    /**
     * Trims the message and then extracts the tokens of the message.
     * Multiple spaces between tokens are considered as one and will not be part of the token.
     * Keys cannot contain spaces, but values can.
     * I.e.
     * <pre>"    put   thisKey     to this   value   "</pre>
     * will produce the following tokens
     * <pre>{"put", "thisKey     to this   value" }</pre>
     *
     * @param message the message from which to extract the tokens
     * @return the extracted tokens of the message
     * @see String#trim()
     */
    static String[] extractTokens(String message) {
        final String trimmedMsg = message.trim();
        String[] msgTokens = trimmedMsg.split("\\s+");
        if (msgTokens.length >= 3 && Constants.PUT_COMMAND.equals(msgTokens[0])) {
            final String[] putAndParameters = trimmedMsg.split("\\s+", 2);
            final String[] parameters = putAndParameters[1].split("\\s+", 2);
            msgTokens = new String[]{putAndParameters[0], parameters[0], parameters[1]};
        }
        return msgTokens;
    }

}
