package de.tum.i13.server.kv;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.server.state.ServerState.State;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPeerAuthenticator {

  static ServerState state;
  static NetworkLocation cur;
  static NetworkLocation ecs;
  static NetworkLocation otherServer;
  static NetworkLocation client;

  @BeforeAll
  public static void createVars() {
    TestPeerAuthenticator.cur = new NetworkLocationImpl("127.0.0.1", 25565);
    TestPeerAuthenticator.ecs = new NetworkLocationImpl("127.0.0.2", 25566);
    TestPeerAuthenticator.otherServer = new NetworkLocationImpl("127.0.0.3", 25567);
    TestPeerAuthenticator.client = new NetworkLocationImpl("127.0.0.4", 25568);
  }

  @BeforeEach
  public void createState() {
    state = new ServerState(cur, ecs, new TreeMapServerMetadata(), State.ACTIVE);
    state.getRingMetadata().addNetworkLocation(cur);
    state.getRingMetadata().addNetworkLocation(otherServer);

  }
 
  @Test
  public void authenticatesEcs() {
    PeerAuthenticator auth = new PeerAuthenticator(state);
    assertThat(auth.authenticate(ecs)).isEqualTo(PeerType.ECS);
  }

  @Test
  public void authenticatesServer() {
    PeerAuthenticator auth = new PeerAuthenticator(state);
    assertThat(auth.authenticate(otherServer)).isEqualTo(PeerType.SERVER);
  }

  @Test
  public void authenticatesClient() {
    PeerAuthenticator auth = new PeerAuthenticator(state);
    assertThat(auth.authenticate(client)).isEqualTo(PeerType.CLIENT);
  }
}
