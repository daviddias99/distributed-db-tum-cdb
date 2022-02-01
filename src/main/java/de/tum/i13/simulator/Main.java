package de.tum.i13.simulator;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

import static de.tum.i13.server.cache.CachingStrategy.LFU;
import static de.tum.i13.simulator.ExperimentConfiguration.experimentConfiguration;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        // cacheExperiment("LFU");
        behaviourExperiment();
    }

    private static void cacheExperiment() throws InterruptedException, IOException {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .afterAdditionalClientsDelay(120)
                .aAfterAdditionalServersDelay(15)
                .build();
        final ExperimentManager experimentManager = startInitialExperiment(experimentConfiguration);
        int timeOffSetFromZero = experimentConfiguration.getInitialDelay();

        timeOffSetFromZero = startAdditionalClients(experimentConfiguration, experimentManager, timeOffSetFromZero);
        timeOffSetFromZero = startAdditionalServers(experimentConfiguration, experimentManager, timeOffSetFromZero);

        (new Thread(new DelayedEvent(timeOffSetFromZero, DelayedEvent.Type.STOP_PROGRAM, experimentManager))).start();
    }

    private static int startAdditionalServers(ExperimentConfiguration experimentConfiguration,
                                              ExperimentManager experimentManager, int timeOffSetFromZero) {
        int serverNumber;
        for (serverNumber = 0; serverNumber < experimentConfiguration.getFinalServerCount() - experimentConfiguration.getStartingServerCount(); serverNumber++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.START_SERVER, experimentManager))).start();
        }
        timeOffSetFromZero = timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay() + experimentConfiguration.getAfterAdditionalServersDelay();
        return timeOffSetFromZero;
    }

    private static int startAdditionalClients(ExperimentConfiguration experimentConfiguration,
                                              ExperimentManager experimentManager, int timeOffSetFromZero) {
        int clientNum = 0;
        // TODO Maybe the second value should be final count - stat count
        for (; clientNum < experimentConfiguration.getFinalClientCount(); clientNum++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + clientNum * experimentConfiguration.getClientStartDelay(), DelayedEvent.Type.START_CLIENT, experimentManager))).start();
        }
        timeOffSetFromZero = timeOffSetFromZero + clientNum * experimentConfiguration.getClientStartDelay() + experimentConfiguration.getAfterAdditionalClientsDelay();
        return timeOffSetFromZero;
    }

    private static ExperimentManager startInitialExperiment(ExperimentConfiguration experimentConfiguration) throws IOException, InterruptedException {
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

    private static ServerManager startInitialServers(ExperimentConfiguration experimentConfiguration) {
        System.out.println("Starting Servers");
        final ServerManager manager = new ServerManager(experimentConfiguration);
        System.out.println("Started Servers");
        return manager;
    }

    private static void behaviourExperiment() throws InterruptedException, IOException {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .startingServerCount(1)
                .startingClientCount(0)
                .finalServerCount(10)
                .finalClientCount(20)
                .serverStartDelay(60)
                .clientStartDelay(20)
                .serverCacheSize(500)
                .bTreeNodeSize(100)
                .serverCachingStrategy(LFU)
                .statsName("behavior")
                .afterAdditionalClientsDelay(120)
                .aAfterAdditionalServersDelay(120)
                .build();
        final ExperimentManager experimentManager = startInitialExperiment(experimentConfiguration);
        int timeOffSetFromZero = experimentConfiguration.getInitialDelay();

        timeOffSetFromZero = startAdditionalClients(experimentConfiguration, experimentManager, timeOffSetFromZero);
        timeOffSetFromZero = startAdditionalServers(experimentConfiguration, experimentManager, timeOffSetFromZero);

        for (int i = 0; i < experimentConfiguration.getFinalServerCount() - 1; i++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + i * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.STOP_SERVER, experimentManager))).start();
        }
    }

    private static ClientManager startInitialClients(ExperimentConfiguration experimentConfiguration, ExperimentManager experimentManager) {
        System.out.println("Creating clients");
        final ClientManager clientManager = new ClientManager(experimentConfiguration, experimentManager);
        System.out.println("Starting clients");
        clientManager.startClients();
        return clientManager;
    }

    private static void startECS() throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(("java -jar target/ecs-server.jar -p 25670 -l logs/ecs.log " +
                "-ll all").split(" "));
        processBuilder.redirectOutput(Redirect.DISCARD);
        processBuilder.redirectError(Redirect.DISCARD);
        System.out.println("Starting ECS");
        Process ecs = processBuilder.start();
        Runtime.getRuntime().addShutdownHook(new Thread(ecs::destroy));
        System.out.println("Started ECS");
    }

}
