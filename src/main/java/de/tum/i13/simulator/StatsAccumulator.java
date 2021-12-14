package de.tum.i13.simulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class StatsAccumulator implements Runnable {

  List<ClientSimulator> clients;
  LinkedList<TimeEvent> timeStats;
  ClientStats accStats;

  public StatsAccumulator() {
    this.timeStats = new LinkedList<>();
    this.accStats = new ClientStats();
  }

  public void setClients(List<ClientSimulator> clients) {
    this.clients = clients;
  }

  @Override
  public void run() {

    int i = 0;
    Runtime.getRuntime().addShutdownHook(new Thread(this::save));

    while (!Thread.interrupted()) {

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      ClientStats currentTimeStats = new ClientStats();

      for (ClientSimulator client : this.clients) {
        synchronized (client.stats) {
          currentTimeStats.add(client.stats);
          client.stats.reset();
        }
      }

      if (i % 50 == 0) {
        this.accStats.print();
      }

      synchronized(this.timeStats) {
        this.timeStats.addLast(currentTimeStats);
      }

      this.accStats.add(currentTimeStats);

      i++;
    }

  }

  private void save(){
    File directory = new File("stats");
    if (! directory.exists()){
        directory.mkdir();
    }

    Random random = new Random();

    File fout = new File(String.format("stats/out_%d.csv", random.nextInt()));
    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fout)))){
      synchronized(this.timeStats) {
        for (TimeEvent timeStep : this.timeStats) {
          bw.write(timeStep.toCSVString());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void signalEvent(TimeEvent event) {
    synchronized(this.timeStats) {
      this.timeStats.addLast(event);
    }
  }
}
