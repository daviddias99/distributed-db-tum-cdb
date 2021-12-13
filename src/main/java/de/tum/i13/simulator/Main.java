package de.tum.i13.simulator;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

public class Main {

  private static final int SERVER_COUNT = 3;
  private static final int SERVER_CACHE_SIZE = 100;
  private static final int BTREE_NODE_SIZE = 100;
  private static final String SERVER_CACHE_STRAT = "LRU";
  private static final int CLIENT_COUNT = 1;

  public static void main(String[] args) throws InterruptedException, IOException {

    ProcessBuilder processBuilder = new ProcessBuilder("java -jar target/ecs-server.jar -p 25670 -l logs/ecs -ll all".split(" "));
    processBuilder.redirectOutput(Redirect.DISCARD);
    processBuilder.redirectError(Redirect.DISCARD);
    System.out.println("Starting ECS");
    Process ecs = processBuilder.start();
    System.out.println("Started ECS");
    System.out.println("Waiting...");
    Thread.sleep(5000);
    System.out.println("Starting Servers");
    final ServerManager manager = new ServerManager(SERVER_COUNT, SERVER_CACHE_SIZE, SERVER_CACHE_STRAT,
    BTREE_NODE_SIZE);
    System.out.println("Started Servers");
    System.out.println("Waiting...");
    Thread.sleep(5000);
    System.out.println("Creating clients");
    final ClientManager clientManager = new ClientManager(CLIENT_COUNT, manager);
    System.out.println("Starting clients");
    clientManager.startClients();

    int i = 2;

    for (; i < 15; i++) {
      (new Thread(new DelayedEvent(i * 5, DelayedEvent.Type.START_CLIENT, manager, clientManager))).start();
    }

    int base = i * 5;
  
    for (i = 1; i < 9; i++) {
      (new Thread(new DelayedEvent(base + i * 8, DelayedEvent.Type.START_SERVER, manager, clientManager))).start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
  }
}
