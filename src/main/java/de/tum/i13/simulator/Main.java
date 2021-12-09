package de.tum.i13.simulator;

import java.io.IOException;


public class Main {

  public static void main(String[] args) {

    final ServerManager manager = new ServerManager(3);
    manager.addServer();

  }
}
