package de.tum.i13.server.kv;

public interface KVMessage {

    enum StatusType {
        GET(true), 			/* Get - request */
        GET_ERROR(true), 		/* requested tuple (i.e. value) not found */
        GET_SUCCESS(false), 	/* requested tuple (i.e. value) found */
        PUT(true), 			/* Put - request */
        PUT_SUCCESS(false), 	/* Put - request successful, tuple inserted */
        PUT_UPDATE(false), 	/* Put - request successful, i.e. value updated */
        PUT_ERROR(false), 		/* Put - request not successful */
        DELETE(true), 		/* Delete - request */
        DELETE_SUCCESS(true), /* Delete - request successful */
        DELETE_ERROR(true); 	/* Delete - request successful */

        private final boolean emptyValue;

        StatusType(boolean emptyValue) {
            this.emptyValue = emptyValue;
        }

        public boolean canHaveEmptyValue() {
            return this.emptyValue;
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
