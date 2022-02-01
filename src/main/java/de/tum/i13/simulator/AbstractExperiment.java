package de.tum.i13.simulator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

abstract class AbstractExperiment implements Experiment {

    private static final Logger LOGGER = LogManager.getLogger(AbstractExperiment.class);

    protected final ExperimentConfiguration cfg;
    protected ExperimentManager mgr;

    protected AbstractExperiment(ExperimentConfiguration cfg) {
        this.cfg = cfg;
    }

    private int startExperiment(int initialTimeOffSetFromZero) throws IOException, InterruptedException {
        startInitialExperiment();
        int timeOffSetFromZero = initialTimeOffSetFromZero + cfg.getInitialDelay();

        timeOffSetFromZero = startAdditionalClients(timeOffSetFromZero);
        timeOffSetFromZero = startAdditionalServers(timeOffSetFromZero);
        return timeOffSetFromZero;
    }

    private int startAdditionalServers(int timeOffSetFromZero) {
        int serverNumber;
        for (serverNumber = 0; serverNumber < cfg.getFinalServerCount() - cfg.getStartingServerCount(); serverNumber++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + serverNumber * cfg.getServerStartDelay(),
                    DelayedEvent.Type.START_SERVER, mgr))).start();
        }
        timeOffSetFromZero =
                timeOffSetFromZero + serverNumber * cfg.getServerStartDelay() + cfg.getAfterAdditionalServersDelay();
        return timeOffSetFromZero;
    }

    private int startAdditionalClients(int timeOffSetFromZero) {
        int clientNum = 0;
        // TODO Maybe the second value should be final count - stat count
        for (; clientNum < cfg.getFinalClientCount(); clientNum++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + clientNum * cfg.getClientStartDelay(),
                    DelayedEvent.Type.START_CLIENT, mgr))).start();
        }
        timeOffSetFromZero =
                timeOffSetFromZero + clientNum * cfg.getClientStartDelay() + cfg.getAfterAdditionalClientsDelay();
        return timeOffSetFromZero;
    }

    private void startInitialExperiment() throws IOException, InterruptedException {
        mgr = new ExperimentManager();

        startECS();
        LOGGER.debug("Waiting...");
        Thread.sleep(4000);

        mgr.setServerManager(startInitialServers());
        LOGGER.debug("Waiting...");
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

    private void startECS() throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(("java -jar target/ecs-server.jar -p 25670 -l logs/ecs.log" +
                " " +
                "-ll all").split(" "));
        processBuilder.redirectOutput(Redirect.DISCARD);
        processBuilder.redirectError(Redirect.DISCARD);
        LOGGER.info("Starting ECS");
        Process ecs = processBuilder.start();
        Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
        LOGGER.info("Started ECS");
    }

    @Override
    public int scheduleRun(int timeOffsetFromZero) {
        try {
            return startExperiment(timeOffsetFromZero);
        } catch (IOException | InterruptedException e) {
            throw new ExperimentException("Could not start experiment", e);
        }
    }

}
