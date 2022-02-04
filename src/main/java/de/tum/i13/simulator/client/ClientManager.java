package de.tum.i13.simulator.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.simulator.events.StatsAccumulator;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import de.tum.i13.simulator.experiments.ExperimentManager;
import de.tum.i13.simulator.server.ServerManager;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Random;

public class ClientManager {

    private static final Logger LOGGER = LogManager.getLogger(ClientManager.class);

    LinkedList<Thread> clientThreads;
    LinkedList<ClientSimulator> clients;
    File[] emailDirs;
    int counter = 0;
    int clientCount;
    public ServerManager servers;
    StatsAccumulator statsAcc;

    private boolean useChord;

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

        this.clientThreads = new LinkedList<>();
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

        for (Thread thread : clientThreads) {
            thread.start();
        }

        this.countStatistics();
    }

    private void countStatistics() {
        this.statsAcc.setClients(this.clients);
        (new Thread(this.statsAcc)).start();
    }

    public Thread addClient(Path emailsPath) {
        Random random = new Random();
        int serverIndex = random.nextInt(servers.servers.size());
        int port = Integer.parseInt(servers.addresses.get(serverIndex).split(" ")[1]);
        ClientSimulator newClient = new ClientSimulator(emailsPath, "127.0.0.1", port, this.useChord);
        this.clients.add(newClient);
        Thread clientThread = new Thread(newClient);
        this.clientThreads.add(clientThread);
        return clientThread;
    }

    public void addAndStartClient() {
        Path path = null;
        do {
            path = Paths.get(emailDirs[counter++].getAbsolutePath(), "all_documents");
        } while (!path.toFile().exists() || path.toFile().listFiles().length < 15);

        this.addClient(path).start();
        LOGGER.trace("Launching client");
        System.out.println("Launching client");
    }

}
