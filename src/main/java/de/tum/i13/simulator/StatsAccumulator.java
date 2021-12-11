package de.tum.i13.simulator;

import java.util.LinkedList;
import java.util.List;

public class StatsAccumulator implements Runnable{

  List<ClientSimulator> clients;
  LinkedList<ClientStats> timeStats;
  ClientStats accStats;

  public StatsAccumulator(List<ClientSimulator> clients) {
    this.clients = clients;
    this.timeStats = new LinkedList<>();
    this.accStats =  new ClientStats();
  }

  @Override
  public void run() {

    while(!Thread.interrupted()) {

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      ClientStats currentTimeStats = new ClientStats();

      for (ClientSimulator client : this.clients) {
        synchronized(client.stats) {
          timeStats.add(client.stats);
          client.stats.reset();
        }
      }

      this.timeStats.addLast(currentTimeStats);
      this.accStats.add(currentTimeStats);
    }
  }
}
