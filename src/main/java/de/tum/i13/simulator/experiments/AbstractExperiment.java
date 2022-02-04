package de.tum.i13.simulator.experiments;

import de.tum.i13.ecs.ExternalConfigurationServer;
import de.tum.i13.simulator.client.ClientManager;
import de.tum.i13.simulator.events.DelayedEvent;
import de.tum.i13.simulator.events.StatsAccumulator;
import de.tum.i13.simulator.server.ServerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.tum.i13.simulator.events.DelayedEvent.Type.START_CLIENT;
import static de.tum.i13.simulator.events.DelayedEvent.Type.START_SERVER;

abstract class AbstractExperiment implements Experiment {

    private static final Logger LOGGER = LogManager.getLogger(AbstractExperiment.class);

    protected final ExperimentConfiguration cfg;
    protected ExperimentManager mgr;

    protected AbstractExperiment(ExperimentConfiguration cfg) {
        this.cfg = cfg;
    }

    private int startExperiment(int initialTimeOffSetFromZero) throws InterruptedException {
        startInitialExperiment();
        int timeOffSetFromZero = initialTimeOffSetFromZero + cfg.getInitialDelay();

        timeOffSetFromZero = startAdditionalClients(timeOffSetFromZero);
        timeOffSetFromZero = startAdditionalServers(timeOffSetFromZero);
        return timeOffSetFromZero;
    }

    private int startAdditionalServers(int timeOffSetFromZero) {
        int serverNumber;
        for (serverNumber = 0; serverNumber < cfg.getFinalServerCount() - cfg.getStartingServerCount(); serverNumber++) {
            new DelayedEvent(timeOffSetFromZero + serverNumber * cfg.getServerStartDelay(), START_SERVER, mgr)
                    .schedule();
        }
        timeOffSetFromZero =
                timeOffSetFromZero + serverNumber * cfg.getServerStartDelay() + cfg.getAfterAdditionalServersDelay();
        return timeOffSetFromZero;
    }

    private int startAdditionalClients(int timeOffSetFromZero) {
        int clientNum = 0;
        // TODO Maybe the second value should be final count - stat count
        for (; clientNum < cfg.getFinalClientCount(); clientNum++) {
            new DelayedEvent(timeOffSetFromZero + clientNum * cfg.getClientStartDelay(), START_CLIENT, mgr)
                    .schedule();
        }
        timeOffSetFromZero =
                timeOffSetFromZero + clientNum * cfg.getClientStartDelay() + cfg.getAfterAdditionalClientsDelay();
        return timeOffSetFromZero;
    }

    private void startInitialExperiment() throws InterruptedException {
        mgr = new ExperimentManager();

        if(!cfg.useChord()) {
            startECS();
            LOGGER.debug("Waiting 4s...");
            Thread.sleep(4000);
        }
        mgr.setServerManager(startInitialServers());
        LOGGER.debug("Waiting 4s...");
        Thread.sleep(4000);

        mgr.setStatsAccumulator(new StatsAccumulator(cfg));
        mgr.setClientManager(startInitialClients());
    }

    private ServerManager startInitialServers() {
        LOGGER.info("Starting servers");
        final ServerManager manager = new ServerManager(cfg);
        LOGGER.info("Started servers");
        return manager;
    }

    private ClientManager startInitialClients() {
        LOGGER.info("Creating clients");
        final ClientManager clientManager = new ClientManager(cfg, mgr);
        LOGGER.info("Starting clients");
        clientManager.startClients();
        return clientManager;
    }

    private void startECS() {
        final Thread ecsThread = new Thread(() -> ExternalConfigurationServer.main(new String[]{
                "-p", "25670",
                "-l", "logs/ec.log"
        }));
        LOGGER.info("Starting ECS");
        ecsThread.start();
        LOGGER.info("Started ECS");
    }

    @Override
    public int scheduleRun(int timeOffsetFromZero) {
        try {
            return startExperiment(timeOffsetFromZero);
        } catch (InterruptedException e) {
            throw new ExperimentException("Could not start experiment", e);
        }
    }

}
