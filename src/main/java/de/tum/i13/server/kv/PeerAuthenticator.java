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
    private final boolean bypasseStop;

    /**
     * A peer type is associated with values that indicate if the peer needs to
     * receive a greet upon connection and if it can still perform actions while a
     * server is in a stopped state.
     * 
     * @param needsGreet  true if peer needs greet
     * @param bypasseStop true if peer can bypass server STOPPED state
     */
    private PeerType(boolean needsGreet, boolean bypasseStop) {
      this.needsGreet = needsGreet;
      this.bypasseStop = bypasseStop;
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
      return this.bypasseStop;
    }
  }

  /**
   * Get peer type from message status type
   * 
   * @param type message status type
   * @return peer type associated with message status type
   */
  public static PeerType authenticate(KVMessage.StatusType type) {
    // TODO: PeerAuthenticator isn't checking all types
    switch (type) {
      case ERROR:
        return PeerType.ANY;
      case GET:
        return PeerType.CLIENT;
      case GET_ERROR:
        return PeerType.SERVER;
      case GET_SUCCESS:
        return PeerType.SERVER;
      case PUT:
        return PeerType.CLIENT;
      case PUT_SERVER:
        return PeerType.SERVER;
      case PUT_ERROR:
        return PeerType.SERVER;
      case PUT_SUCCESS:
        return PeerType.SERVER;
      case PUT_UPDATE:
        return PeerType.SERVER;
      case DELETE:
        return PeerType.CLIENT;
      case DELETE_SUCCESS:
        return PeerType.SERVER;
      case DELETE_ERROR:
        return PeerType.SERVER;
      case SERVER_STOPPED:
        return PeerType.SERVER;
      case SERVER_HANDOFF_SUCCESS:
        return PeerType.SERVER;
      case SERVER_NOT_RESPONSIBLE:
        return PeerType.SERVER;
      case SERVER_START:
        return PeerType.SERVER;
      case SERVER_ACK:
        return PeerType.SERVER;
      case SERVER_WRITE_LOCK:
        return PeerType.SERVER;
      case SERVER_SHUTDOWN:
        return PeerType.SERVER;
      case SERVER_HEART_BEAT:
        return PeerType.SERVER;
      case ECS_WRITE_LOCK:
        return PeerType.ECS;
      case ECS_WRITE_UNLOCK:
        return PeerType.ECS;
      case ECS_HANDOFF:
        return PeerType.ECS;
      case ECS_SET_KEYRANGE:
        return PeerType.ECS;
      case ECS_HEART_BEAT:
        return PeerType.ECS;
      case KEYRANGE:
        return PeerType.CLIENT;
      case KEYRANGE_SUCCESS:
        return PeerType.SERVER;
      case CHORD_CLOSEST_PRECEDING_FINGER:
        return PeerType.SERVER;
      case CHORD_FIND_SUCCESSOR:
        return PeerType.SERVER;
      case CHORD_GET_PREDECESSOR:
        return PeerType.SERVER;
      case CHORD_NOTIFY:
        return PeerType.SERVER;
      case CHORD_GET_STATE_STR:
        return PeerType.SERVER;
      default:
        return PeerType.ANY;
    }
  }
}
