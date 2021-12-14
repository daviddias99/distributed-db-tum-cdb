package de.tum.i13.simulator;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

public class Main {

  private static final int STARTING_SERVER_COUNT = 1;
  private static final int STARTING_CLIENT_COUNT = 0;
  private static final int FINAL_SERVER_COUNT = 5;
  private static final int FINAL_CLIENT_COUNT = 10;
  private static final int SERVER_START_DELAY = 20;
  private static final int CLIENT_START_DELAY = 10;

  private static final int SERVER_CACHE_SIZE = 100;
  private static final int BTREE_NODE_SIZE = 1000;
  private static final String SERVER_CACHE_STRAT = "LRU";

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
    final ServerManager manager = new ServerManager(STARTING_SERVER_COUNT, SERVER_CACHE_SIZE, SERVER_CACHE_STRAT,
    BTREE_NODE_SIZE);
    System.out.println("Started Servers");
    System.out.println("Waiting...");
    Thread.sleep(5000);
    System.out.println("Creating clients");
    StatsAccumulator acc = new StatsAccumulator();
    final ClientManager clientManager = new ClientManager(STARTING_CLIENT_COUNT, manager, acc);
    System.out.println("Starting clients");
    clientManager.startClients();

    int base = 1;

    int i = 0;

    for (; i < FINAL_CLIENT_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * CLIENT_START_DELAY, DelayedEvent.Type.START_CLIENT, manager, clientManager, acc))).start();
    }

    base = base + i * CLIENT_START_DELAY + 5;
  
    for (i = 0; i < FINAL_SERVER_COUNT - STARTING_SERVER_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * SERVER_START_DELAY, DelayedEvent.Type.START_SERVER, manager, clientManager, acc))).start();
    }

    base = base + i * SERVER_START_DELAY + 5;

    for (i = 0; i < FINAL_SERVER_COUNT - 1; i++) {
      (new Thread(new DelayedEvent(base + i * SERVER_START_DELAY, DelayedEvent.Type.STOP_SERVER, manager, clientManager, acc))).start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
  }
}
