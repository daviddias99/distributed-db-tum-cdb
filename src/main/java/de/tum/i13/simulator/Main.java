package de.tum.i13.simulator;

public class Main {

  public static void main(String[] args) {

    final ServerManager manager = new ServerManager(3);
    manager.addServer();
    final ClientManager clientManager = new ClientManager(3);
  }
}
