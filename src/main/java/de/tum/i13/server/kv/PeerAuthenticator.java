package de.tum.i13.server.kv;

import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.net.NetworkLocation;

public class PeerAuthenticator {

  ServerState currentServerState;

  public enum PeerType {
    CLIENT(true, false),
    SERVER(false, true),
    ECS(false, true);

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

  public PeerAuthenticator(ServerState serverState) {
    this.currentServerState = serverState;
  }

  public PeerType authenticate(NetworkLocation location) {

    if(this.currentServerState.getEcsLocation().equals(location)) {
      return PeerType.ECS;
    } else if (this.currentServerState.getRingMetadata().containsNetworkLocation(location)) {
      return PeerType.SERVER;
    }
    
    return PeerType.CLIENT;
  }
}
