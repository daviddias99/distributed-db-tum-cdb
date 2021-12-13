package de.tum.i13.simulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class StatsAccumulator implements Runnable {

  List<ClientSimulator> clients;
  LinkedList<ClientStats> timeStats;
  ClientStats accStats;

  public StatsAccumulator(List<ClientSimulator> clients) {
    this.clients = clients;
    this.timeStats = new LinkedList<>();
    this.accStats = new ClientStats();
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

      this.timeStats.addLast(currentTimeStats);
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
    try {
      FileOutputStream fos = new FileOutputStream(fout);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

      for (ClientStats timeStep : this.timeStats) {
        bw.write(timeStep.toCSVString());
      }
  
      bw.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
