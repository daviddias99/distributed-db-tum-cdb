package de.tum.i13.simulator.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.tum.i13.simulator.events.StatsAccumulator;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import de.tum.i13.simulator.experiments.ExperimentManager;
import de.tum.i13.simulator.server.ServerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static de.tum.i13.simulator.SimulatorUtils.withLoggedToDifferentFiles;
import static de.tum.i13.simulator.SimulatorUtils.wrapWarnLogging;

public class ClientManager {

    private static final Logger LOGGER = LogManager.getLogger(ClientManager.class);

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("client-pool_%d").build()
    );
    private static final Random RANDOM = new Random();
    public ServerManager servers;
    List<ClientSimulator> clients;
    File[] emailDirs;
    int counter = 0;
    int clientCount;
    StatsAccumulator statsAcc;

    private final boolean useChord;

    public ClientManager(ExperimentConfiguration experimentConfiguration, ExperimentManager experimentManager) {
        this(
                experimentConfiguration.getStartingClientCount(),
                experimentConfiguration.useChord(),
                experimentManager.getServerManager(),
                experimentManager.getStatsAccumulator()
        );
    }

    public ClientManager(int count, boolean useChord, ServerManager servers, StatsAccumulator statsAcc) {
        this.servers = servers;
        emailDirs = Paths.get("maildir").toFile().listFiles();

        this.clients = new LinkedList<>();
        this.clientCount = count;
        this.statsAcc = statsAcc;
        this.useChord = useChord;
        this.createClients();
    }

    private void createClients() {
        for (int i = 0; i < clientCount; i++) {

            Path path = null;

            do {
                path = Paths.get(emailDirs[counter++].getAbsolutePath(), "all_documents");
            } while (!path.toFile().exists() || path.toFile().listFiles().length < 15);

            this.addClient(path);
        }
    }

    public void startClients() {
        clients.forEach(this::startClient);
        this.countStatistics();
    }

    private void countStatistics() {
        this.statsAcc.setClients(this.clients);
        EXECUTOR_SERVICE.submit(this.statsAcc);
    }

    public ClientSimulator addClient(Path emailsPath) {
        int serverIndex = RANDOM.nextInt(servers.servers.size());
        int port = Integer.parseInt(servers.addresses.get(serverIndex).split(" ")[1]);
        ClientSimulator newClient = new ClientSimulator(emailsPath, "127.0.0.1", port, this.useChord);
        this.clients.add(newClient);
        return newClient;
    }

    public void addAndStartClient() {
        Path path = null;
        do {
            path = Paths.get(emailDirs[counter++].getAbsolutePath(), "all_documents");
        } while (!path.toFile().exists() || path.toFile().listFiles().length < 15);

        startClient(this.addClient(path));
        LOGGER.trace("Launching client");
    }

    private void startClient(ClientSimulator clientSimulator) {
        EXECUTOR_SERVICE.submit(() -> {
            final Path logFile = Path.of("logs", Thread.currentThread().getName() + ".log");
            wrapWarnLogging(withLoggedToDifferentFiles(clientSimulator, logFile))
                    .run();
        });
    }

}
