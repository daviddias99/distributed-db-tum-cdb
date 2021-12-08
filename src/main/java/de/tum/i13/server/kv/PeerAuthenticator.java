package de.tum.i13.server.kv;

public class PeerAuthenticator {

  public enum PeerType {
    CLIENT(true, false),
    SERVER(false, true),
    ECS(false, true),
    ANY(false, true);

    private final boolean needsGreet;
    private final boolean bypasseStop;

    private PeerType(boolean needsGreet, boolean bypasseStop) {
      this.needsGreet = needsGreet;
      this.bypasseStop = bypasseStop;
    }

    public boolean needsGreet() {
      return needsGreet;
    }

    public boolean canBypassStop() {
      return this.bypasseStop;
    }
  }

  public static PeerType authenticate(KVMessage.StatusType type) {
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
      case SERVER_GET_METADATA:
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
      default:
        return PeerType.ANY;
    }
  }
}
