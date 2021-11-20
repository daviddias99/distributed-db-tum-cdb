package de.tum.i13.server.kv;

import java.util.Objects;

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
        UNDEFINED(true),
        /**
         * Get - request
         */
        GET(true),
        /**
         * requested tuple (i.e. value) not found
         */
        GET_ERROR(true),
        /**
         * requested tuple (i.e. value) found
         */
        GET_SUCCESS(false),
        // TODO Why is an empty value possible here
        /**
         * Put - request
         */
        PUT(true),
        /**
         * Put - request successful, tuple inserted
         */
        PUT_SUCCESS(false),
        /**
         * Put - request successful, i.e. value updated
         */
        PUT_UPDATE(false),
        /**
         * Put - request not successful
         */
        PUT_ERROR(false),
        /**
         * Delete - request
         */
        DELETE(true),
        /**
         * Delete - request successful
         */
        DELETE_SUCCESS(true),
        /**
         * Delete - request successful
         */
        DELETE_ERROR(true);

        private final boolean canHavEmptyValue;

        StatusType(boolean canHavEmptyValue) {
            this.canHavEmptyValue = canHavEmptyValue;
        }

        /**
         * Check if this type of {@link KVMessage} can have an empty value
         *
         * @return if it can have an empty value
         */
        public boolean canHaveEmptyValue() {
            return this.canHavEmptyValue;
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

    default String packMessage() {
        return String.format("%s: %s %s", getStatus(), getKey(), Objects.toString(getValue(), ""));
    }

    // TODO Make more robust
    // TODO Make case insenstive
    // TODO Consider spaces in values
    // TODO Use in server
    static KVMessage unpackMessage(String message) {
        final String[] msgTokens = message.trim().split("\\s+");
        return new KVMessageImpl(msgTokens[1], msgTokens[2], StatusType.valueOf(msgTokens[0]));
    }

}
