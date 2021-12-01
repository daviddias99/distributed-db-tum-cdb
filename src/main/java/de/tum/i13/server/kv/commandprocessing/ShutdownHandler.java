package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.NetworkMessageServer;
import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.shared.NetworkLocation;

public class ShutdownHandler implements Runnable {

  private NetworkLocation ecs;
  public ShutdownHandler(NetworkLocation ecs) {
    this.ecs = ecs;
  }

  public void run() {
    NetworkMessageServer messageServer = new CommunicationClient();
    ServerCommunicator communicator = new ServerCommunicator(messageServer);

    try {
      communicator.connect(ecs.getAddress(), ecs.getPort());
      communicator.sendShutdown();
    } catch (ClientException e) {
      // TODO Auto-generated catch block
    }
  }
}
