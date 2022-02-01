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
                .build();
        startECS();
        System.out.println("Waiting...");
        Thread.sleep(4000);
        final ServerManager manager = startServers(experimentConfiguration);
        System.out.println("Waiting...");
        Thread.sleep(4000);
        final StatsAccumulator acc = new StatsAccumulator(experimentConfiguration);
        final ClientManager clientManager = startClients(experimentConfiguration, manager, acc);

        int base = 60;

        int i = 0;

        for (; i < experimentConfiguration.getFinalClientCount(); i++) {
            (new Thread(new DelayedEvent(base + i * experimentConfiguration.getClientStartDelay(), DelayedEvent.Type.START_CLIENT, manager,
                    clientManager, acc))).start();
        }

        base = base + i * experimentConfiguration.getClientStartDelay() + 120;

        for (i = 0; i < experimentConfiguration.getFinalServerCount() - experimentConfiguration.getStartingServerCount(); i++) {
            (new Thread(new DelayedEvent(base + i * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.START_SERVER, manager,
                    clientManager, acc))).start();
        }
        base = base + i * experimentConfiguration.getServerStartDelay() + 15;

        (new Thread(new DelayedEvent(base, DelayedEvent.Type.STOP_PROGRAM, manager, clientManager, acc))).start();
    }

    private static ServerManager startServers(ExperimentConfiguration experimentConfiguration) {
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
                .build();

        startECS();
        System.out.println("Waiting...");
        Thread.sleep(4000);
        final ServerManager manager = startServers(experimentConfiguration);
        System.out.println("Waiting...");
        Thread.sleep(4000);
        final StatsAccumulator acc = new StatsAccumulator(experimentConfiguration);
        final ClientManager clientManager = startClients(experimentConfiguration, manager, acc);

        int base = 60;

        int i = 0;

        for (; i < experimentConfiguration.getFinalClientCount(); i++) {
            (new Thread(new DelayedEvent(base + i * experimentConfiguration.getClientStartDelay(), DelayedEvent.Type.START_CLIENT, manager,
                    clientManager, acc))).start();
        }

        base = base + i * experimentConfiguration.getClientStartDelay() + 120;

        for (i = 0; i < experimentConfiguration.getFinalServerCount() - experimentConfiguration.getStartingServerCount(); i++) {
            (new Thread(new DelayedEvent(base + i * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.START_SERVER, manager, clientManager, acc))).start();
        }

        base = base + i * experimentConfiguration.getServerStartDelay() + 120;

        for (i = 0; i < experimentConfiguration.getFinalServerCount() - 1; i++) {
            (new Thread(new DelayedEvent(base + i * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.STOP_SERVER, manager, clientManager, acc))).start();
        }
    }

    private static ClientManager startClients(ExperimentConfiguration experimentConfiguration, ServerManager manager, StatsAccumulator acc) {
        System.out.println("Creating clients");
        final ClientManager clientManager = new ClientManager(experimentConfiguration, manager, acc);
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
