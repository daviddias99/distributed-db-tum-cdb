package de.tum.i13.simulator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

abstract class AbstractExperiment implements Experiment {

    private static final Logger LOGGER = LogManager.getLogger(AbstractExperiment.class);

    protected final ExperimentConfiguration experimentConfiguration;
    protected ExperimentManager experimentManager;

    protected AbstractExperiment(ExperimentConfiguration experimentConfiguration) {
        this.experimentConfiguration = experimentConfiguration;
    }

    private int startExperiment(ExperimentConfiguration experimentConfiguration, int initialTimeOffSetFromZero) throws IOException, InterruptedException {
        experimentManager = startInitialExperiment(experimentConfiguration);
        int timeOffSetFromZero = initialTimeOffSetFromZero + experimentConfiguration.getInitialDelay();

        timeOffSetFromZero = startAdditionalClients(experimentConfiguration, experimentManager, timeOffSetFromZero);
        timeOffSetFromZero = startAdditionalServers(experimentConfiguration, experimentManager, timeOffSetFromZero);
        return timeOffSetFromZero;
    }

    private int startAdditionalServers(ExperimentConfiguration experimentConfiguration,
                                              ExperimentManager experimentManager, int timeOffSetFromZero) {
        int serverNumber;
        for (serverNumber = 0; serverNumber < experimentConfiguration.getFinalServerCount() - experimentConfiguration.getStartingServerCount(); serverNumber++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.START_SERVER, experimentManager))).start();
        }
        timeOffSetFromZero = timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay() + experimentConfiguration.getAfterAdditionalServersDelay();
        return timeOffSetFromZero;
    }

    private int startAdditionalClients(ExperimentConfiguration experimentConfiguration,
                                              ExperimentManager experimentManager, int timeOffSetFromZero) {
        int clientNum = 0;
        // TODO Maybe the second value should be final count - stat count
        for (; clientNum < experimentConfiguration.getFinalClientCount(); clientNum++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + clientNum * experimentConfiguration.getClientStartDelay(), DelayedEvent.Type.START_CLIENT, experimentManager))).start();
        }
        timeOffSetFromZero = timeOffSetFromZero + clientNum * experimentConfiguration.getClientStartDelay() + experimentConfiguration.getAfterAdditionalClientsDelay();
        return timeOffSetFromZero;
    }

    private ExperimentManager startInitialExperiment(ExperimentConfiguration experimentConfiguration) throws IOException, InterruptedException {
        final var experimentManager = new ExperimentManager();
        startECS();
        System.out.println("Waiting...");
        Thread.sleep(4000);
        experimentManager.setServerManager(startInitialServers(experimentConfiguration));
        System.out.println("Waiting...");
        Thread.sleep(4000);
        experimentManager.setStatsAccumulator(new StatsAccumulator(experimentConfiguration));
        experimentManager.setClientManager(startInitialClients(experimentConfiguration, experimentManager));
        return experimentManager;
    }

    private ServerManager startInitialServers(ExperimentConfiguration experimentConfiguration) {
        System.out.println("Starting Servers");
        final ServerManager manager = new ServerManager(experimentConfiguration);
        System.out.println("Started Servers");
        return manager;
    }

    private ClientManager startInitialClients(ExperimentConfiguration experimentConfiguration, ExperimentManager experimentManager) {
        System.out.println("Creating clients");
        final ClientManager clientManager = new ClientManager(experimentConfiguration, experimentManager);
        System.out.println("Starting clients");
        clientManager.startClients();
        return clientManager;
    }

    private void startECS() throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(("java -jar target/ecs-server.jar -p 25670 -l logs/ecs.log " +
                "-ll all").split(" "));
        processBuilder.redirectOutput(Redirect.DISCARD);
        processBuilder.redirectError(Redirect.DISCARD);
        System.out.println("Starting ECS");
        Process ecs = processBuilder.start();
        Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
        System.out.println("Started ECS");
    }

    @Override
    public int scheduleRun(int timeOffsetFromZero) {
        try {
            return startExperiment(experimentConfiguration, timeOffsetFromZero);
        } catch (IOException | InterruptedException e) {
            throw new ExperimentException("Could not start experiment", e);
        }
    }

}
