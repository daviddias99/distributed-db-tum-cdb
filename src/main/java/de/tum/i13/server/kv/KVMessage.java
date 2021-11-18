package de.tum.i13.server.kv;

/**
 * A message that is interchanged between a {@link KVStore} and a caller
 */
public interface KVMessage {

    /**
     * The type and status of the {@link KVMessage}
     */
    enum StatusType {
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

}
