package de.tum.i13.simulator;

public class Main {

  private static final int SERVER_COUNT = 10;
  private static final int SERVER_CACHE_SIZE = 100;
  private static final int BTREE_NODE_SIZE = 100;
  private static final String SERVER_CACHE_STRAT = "LRU";
  private static final int CLIENT_COUNT = 20;

  public static void main(String[] args) {
    final ServerManager manager = new ServerManager(SERVER_COUNT, SERVER_CACHE_SIZE, SERVER_CACHE_STRAT, BTREE_NODE_SIZE);
    final ClientManager clientManager = new ClientManager(CLIENT_COUNT, manager);
  }
}
