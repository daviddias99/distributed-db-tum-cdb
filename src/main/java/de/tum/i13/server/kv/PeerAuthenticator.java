package de.tum.i13.server.kv;

/**
 * A peer authenticator is used to translate a {@link KVMessage.StatusType} to a
 * {@link PeerType}
 */
public class PeerAuthenticator {

    /**
     * Type of a peer
     */
    public enum PeerType {
        /**
         * client
         */
        CLIENT(true, false),

        /**
         * server
         */
        SERVER(false, true),

        /**
         * ECS
         */
        ECS(false, true),

        /**
         * Any of the previous values
         */
        ANY(false, true);

        private final boolean needsGreet;
        private final boolean bypassesStop;

        /**
         * A peer type is associated with values that indicate if the peer needs to
         * receive a greet upon connection and if it can still perform actions while a
         * server is in a stopped state.
         *
         * @param needsGreet   true if peer needs greet
         * @param bypassesStop true if peer can bypass server STOPPED state
         */
        private PeerType(boolean needsGreet, boolean bypassesStop) {
            this.needsGreet = needsGreet;
            this.bypassesStop = bypassesStop;
        }

        /**
         * Check if peer type needs server greet
         *
         * @return true if peer needs server greet
         */
        public boolean needsGreet() {
            return needsGreet;
        }

        /**
         * Check if peer type can bypass server stopped state
         *
         * @return true if peer can bypass server stopped state
         */
        public boolean canBypassStop() {
            return this.bypassesStop;
        }
    }

    /**
     * Get peer type from message status type
     *
     * @param type message status type
     * @return peer type associated with message status type
     */
    public static PeerType authenticate(KVMessage.StatusType type) {
        return switch (type) {
            case GET, PUT, KEYRANGE, DELETE -> PeerType.CLIENT;
            case SERVER_HANDOFF_ACK, SERVER_WRITE_UNLOCK, GET_ERROR, GET_SUCCESS, PUT_SERVER, PUT_ERROR, PUT_SUCCESS,
                    PUT_UPDATE, DELETE_SUCCESS, DELETE_ERROR, SERVER_STOPPED, SERVER_HANDOFF_SUCCESS,
                    SERVER_NOT_RESPONSIBLE, SERVER_START, SERVER_ACK, SERVER_WRITE_LOCK, SERVER_SHUTDOWN,
                    SERVER_HEART_BEAT, KEYRANGE_SUCCESS, CHORD_CLOSEST_PRECEDING_FINGER, CHORD_FIND_SUCCESSOR,
                    CHORD_GET_PREDECESSOR, CHORD_NOTIFY, CHORD_GET_STATE_STR, CHORD_HEARTBEAT,
                    CHORD_HEARTBEAT_RESPONSE, CHORD_FIND_SUCESSSOR_RESPONSE, CHORD_CLOSEST_PRECEDING_FINGER_RESPONSE,
                    CHORD_GET_PREDECESSOR_RESPONSE, CHORD_GET_SUCCESSORS, CHORD_GET_SUCCESSOR_RESPONSE,
                    CHORD_NOTIFY_ACK -> PeerType.SERVER;
            case ECS_WRITE_LOCK, ECS_WRITE_UNLOCK, ECS_HANDOFF, ECS_SET_KEYRANGE, ECS_HEART_BEAT, ECS_ACK,
                    ECS_WAITING_FOR_HANDOFF -> PeerType.ECS;
            case ERROR, CHORD_GET_STATE_STR_RESPONSE -> PeerType.ANY;
        };
    }

}
