package de.tum.i13.simulator.events;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.tum.i13.simulator.client.ClientManager;
import de.tum.i13.simulator.experiments.ExperimentManager;
import de.tum.i13.simulator.server.ServerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DelayedEvent implements Runnable, TimeEvent {

    private static final Logger LOGGER = LogManager.getLogger(DelayedEvent.class);

    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(
            5,
            new ThreadFactoryBuilder().setNameFormat("delayed-event-pool_%d").build()
    );

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
        LOGGER.trace("Running scheduled event: '{}'", this);
        acc.signalEvent(this);
        switch (this.eType) {
            case START_SERVER -> sManager.addServer();
            case STOP_SERVER -> sManager.stopServer();
            case START_CLIENT -> cManager.addAndStartClient();
            case STOP_PROGRAM -> System.exit(0);
        }
    }

    @Override
    public String toCSVString() {
        return this.eType.name() + ",,,,,,,,\n";
    }

    @Override
    public String toString() {
        return "DelayedEvent{" +
                "type=" + this.eType.name() +
                ", timeSeconds=" + this.timeSeconds +
                "}";
    }

    public void schedule() {
        EXECUTOR_SERVICE.schedule(this, timeSeconds, TimeUnit.SECONDS);
    }

    public enum Type {
        START_SERVER,
        STOP_SERVER,
        START_CLIENT,
        STOP_PROGRAM
    }

}
