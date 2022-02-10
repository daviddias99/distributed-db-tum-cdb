package de.tum.i13.simulator.events;

import de.tum.i13.simulator.client.ClientSimulator;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class StatsAccumulator implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(StatsAccumulator.class);

    List<ClientSimulator> clients;
    LinkedList<ClientStats> timeStats;
    ClientStats accStats;
    DelayedEvent event;
    String name;

    public StatsAccumulator(ExperimentConfiguration experimentConfiguration) {
        this(experimentConfiguration.getStatsName());
    }

    public StatsAccumulator(String name) {
        this.timeStats = new LinkedList<>();
        this.accStats = new ClientStats();
        this.name = name;
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

                    if (this.event != null) {
                        currentTimeStats.periodEvent = this.event;
                        this.event = null;
                    }

                    currentTimeStats.add(client.stats);
                    client.stats.reset();
                }
            }

            if (i % 30 == 0) {
                this.accStats.print();
            }

            currentTimeStats.timeStep = this.timeStats.size() + 1;
            synchronized (this.timeStats) {
                this.timeStats.addLast(currentTimeStats);
            }

            this.accStats.add(currentTimeStats);

            i++;
        }

    }

    private void save() {
        File directory = new File("stats");
        if (!directory.exists()) {
            directory.mkdir();
        }

        File fOut = new File(String.format("stats/out_%s_%s.csv", this.name, new SimpleDateFormat("yyyy-MM-dd-HH-mm" +
                "-ss").format(new Date())));
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fOut)))) {
            bw.write("timeStep,getCount,getFailCount,getTime,putCount,putFailCount,putTime,deleteCount," +
                    "deleteFailCount,deleteTime,totalSucc,event\n");
            synchronized (this.timeStats) {
                for (TimeEvent timeStep : this.timeStats) {
                    bw.write(timeStep.toCSVString());
                }
            }
        } catch (IOException e) {
            LOGGER.error("Caught exception while saving stats", e);
        }
    }

    public void signalEvent(DelayedEvent event) {
        this.event = event;
    }

}
