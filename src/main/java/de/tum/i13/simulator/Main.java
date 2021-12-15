package de.tum.i13.simulator;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

public class Main {

  public static void main(String[] args) throws InterruptedException, IOException {
    // cacheExperiment("LFU");
    behaviourExperiment();
  }

  private static void cacheExperiment(String strat) throws InterruptedException, IOException {
    final int STARTING_SERVER_COUNT = 1;
    final int STARTING_CLIENT_COUNT = 0;
    final int FINAL_SERVER_COUNT = 5;
    final int FINAL_CLIENT_COUNT = 10;
    final int SERVER_START_DELAY = 90;
    final int CLIENT_START_DELAY = 20;
  
    final int SERVER_CACHE_SIZE = 500;
    final int BTREE_NODE_SIZE = 100;
    final String SERVER_CACHE_STRAT = strat;

    ProcessBuilder processBuilder = new ProcessBuilder("java -jar target/ecs-server.jar -p 25670 -l logs/ecs.log -ll all".split(" "));
    processBuilder.redirectOutput(Redirect.DISCARD);
    processBuilder.redirectError(Redirect.DISCARD);
    System.out.println("Starting ECS");
    Process ecs = processBuilder.start();
    Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
    System.out.println("Started ECS");
    System.out.println("Waiting...");
    Thread.sleep(4000);
    System.out.println("Starting Servers");
    final ServerManager manager = new ServerManager(STARTING_SERVER_COUNT, SERVER_CACHE_SIZE, SERVER_CACHE_STRAT, BTREE_NODE_SIZE);
    System.out.println("Started Servers");
    System.out.println("Waiting...");
    Thread.sleep(4000);
    System.out.println("Creating clients");
    StatsAccumulator acc = new StatsAccumulator(strat);
    final ClientManager clientManager = new ClientManager(STARTING_CLIENT_COUNT, manager, acc);
    System.out.println("Starting clients");
    clientManager.startClients();

    int base = 60;

    int i = 0;

    for (; i < FINAL_CLIENT_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * CLIENT_START_DELAY, DelayedEvent.Type.START_CLIENT, manager, clientManager, acc))).start();
    }

    base = base + i * CLIENT_START_DELAY + 120;
  
    for (i = 0; i < FINAL_SERVER_COUNT - STARTING_SERVER_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * SERVER_START_DELAY, DelayedEvent.Type.START_SERVER, manager, clientManager, acc))).start();
    }
    base = base + i * SERVER_START_DELAY + 15;

    (new Thread(new DelayedEvent(base, DelayedEvent.Type.STOP_PROGRAM, manager, clientManager, acc))).start();
  }

  private static void behaviourExperiment() throws InterruptedException, IOException {
    final int STARTING_SERVER_COUNT = 1;
    final int STARTING_CLIENT_COUNT = 0;
    final int FINAL_SERVER_COUNT = 10;
    final int FINAL_CLIENT_COUNT = 20;
    final int SERVER_START_DELAY = 60;
    final int CLIENT_START_DELAY = 20;
  
    final int SERVER_CACHE_SIZE = 500;
    final int BTREE_NODE_SIZE = 100;
    final String SERVER_CACHE_STRAT = "LFU";

    ProcessBuilder processBuilder = new ProcessBuilder("java -jar target/ecs-server.jar -p 25670 -l logs/ecs.log -ll all".split(" "));
    processBuilder.redirectOutput(Redirect.DISCARD);
    processBuilder.redirectError(Redirect.DISCARD);
    System.out.println("Starting ECS");
    Process ecs = processBuilder.start();
    Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
    System.out.println("Started ECS");
    System.out.println("Waiting...");
    Thread.sleep(4000);
    System.out.println("Starting Servers");
    final ServerManager manager = new ServerManager(STARTING_SERVER_COUNT, SERVER_CACHE_SIZE, SERVER_CACHE_STRAT, BTREE_NODE_SIZE);
    System.out.println("Started Servers");
    System.out.println("Waiting...");
    Thread.sleep(4000);
    System.out.println("Creating clients");
    StatsAccumulator acc = new StatsAccumulator("behaviour");
    final ClientManager clientManager = new ClientManager(STARTING_CLIENT_COUNT, manager, acc);
    System.out.println("Starting clients");
    clientManager.startClients();

    int base = 60;

    int i = 0;

    for (; i < FINAL_CLIENT_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * CLIENT_START_DELAY, DelayedEvent.Type.START_CLIENT, manager, clientManager, acc))).start();
    }

    base = base + i * CLIENT_START_DELAY + 120;
  
    for (i = 0; i < FINAL_SERVER_COUNT - STARTING_SERVER_COUNT; i++) {
      (new Thread(new DelayedEvent(base + i * SERVER_START_DELAY, DelayedEvent.Type.START_SERVER, manager, clientManager, acc))).start();
    }

    base = base + i * SERVER_START_DELAY + 120;

    for (i = 0; i < FINAL_SERVER_COUNT - 1; i++) {
      (new Thread(new DelayedEvent(base + i * SERVER_START_DELAY, DelayedEvent.Type.STOP_SERVER, manager, clientManager, acc))).start();
    }
  }
}
