package de.tum.i13.server.kv;

import de.tum.i13.shared.ActiveConnection;

public class PeerAuthenticator {

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

  public PeerAuthenticator() {
    // Add authentication based on metadata, methods may need to become static
  }

  public PeerType authenticate(ActiveConnection activeConnection) {

    return PeerType.CLIENT;
  }
}
