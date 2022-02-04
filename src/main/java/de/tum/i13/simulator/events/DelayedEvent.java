package de.tum.i13.simulator.events;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.simulator.client.ClientManager;
import de.tum.i13.simulator.experiments.ExperimentManager;
import de.tum.i13.simulator.server.ServerManager;

public class DelayedEvent implements Runnable, TimeEvent {

    private static final Logger LOGGER = LogManager.getLogger(DelayedEvent.class);

    private final int timeSeconds;
    private final Type eType;
    private final ServerManager sManager;
    private final ClientManager cManager;
    private final StatsAccumulator acc;

    public DelayedEvent(int timeSeconds, Type eType, ExperimentManager experimentManager) {
        this(
                timeSeconds,
                eType,
                experimentManager.getServerManager(),
                experimentManager.getClientManager(),
                experimentManager.getStatsAccumulator()
        );
    }


    public DelayedEvent(int timeSeconds, Type eType, ServerManager sManager, ClientManager cManager,
                        StatsAccumulator acc) {

        this.eType = eType;
        this.timeSeconds = timeSeconds;
        this.sManager = sManager;
        this.cManager = cManager;
        this.acc = acc;
    }

    @Override
    public void run() {
        try {
            Thread.sleep((long) timeSeconds * 1000);

            acc.signalEvent(this);
            switch (this.eType) {
                case START_SERVER -> sManager.addServer();
                case STOP_SERVER -> sManager.stopServer();
                case START_CLIENT -> cManager.addAndStartClient();
                case STOP_PROGRAM -> System.exit(0);
            }

        } catch (InterruptedException e) {
            LOGGER.error("Interrupted delayed event");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toCSVString() {
        return this.eType.name() + ",,,,,,,,\n";
    }

    @Override
    public String toString() {
        return this.eType.name();
    }

    public void schedule() {
        new Thread(this).start();
    }

    public enum Type {
        START_SERVER,
        STOP_SERVER,
        START_CLIENT,
        STOP_PROGRAM
    }

}
