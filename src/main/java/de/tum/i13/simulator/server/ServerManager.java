package de.tum.i13.simulator.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.server.threadperconnection.MainChord;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static de.tum.i13.simulator.SimulatorUtils.wrapWarnLogging;

public class ServerManager {

    private static final Logger LOGGER = LogManager.getLogger(ServerManager.class);

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("server-pool_%d").build()
    );
    public static final Random RANDOM = new Random();

    public List<Future<?>> servers;
    public List<String> addresses;
    int port = 35660;
    String ecsAddress = "127.0.0.1:25670";

    int cacheSize;
    CachingStrategy cacheStrategy;
    int bTreeNodeSize;
    int replicationFactor;
    boolean useChord;

    public ServerManager(ExperimentConfiguration experimentConfiguration) {
        this(
                experimentConfiguration.getStartingServerCount(),
                experimentConfiguration.getServerCacheSize(),
                experimentConfiguration.getServerCachingStrategy(),
                experimentConfiguration.getbTreeNodeSize(),
                experimentConfiguration.useChord(),
                experimentConfiguration.getReplicationFactor()
        );
    }

    public ServerManager(int count, int cacheSize, CachingStrategy cacheStrategy, int bTreeNodeSize, boolean useChord, int replicationFactor) {

        this.servers = new LinkedList<>();
        this.addresses = new LinkedList<>();

        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.bTreeNodeSize = bTreeNodeSize;
        this.useChord = useChord;
        this.replicationFactor = replicationFactor;

        for (int i = 0; i < count; i++) {
            this.addServer();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private List<String> getServerParameters() {
        final List<String> parameters = new ArrayList<>();
        final String dataDir = Paths.get("data", Integer.toString(port)).toString();

        if (!this.useChord) {
            Stream.of("-b", ecsAddress)
                    .forEachOrdered(parameters::add);
        } else if (!this.addresses.isEmpty()) {
            String address = this.addresses.get(RANDOM.nextInt(this.addresses.size()));
            Stream.of("-b", address.replace(" ", ":"))
                    .forEachOrdered(parameters::add);
        }
        Stream.of(
                "-ll", "ALL",
                "-t", bTreeNodeSize,
                "-p", port,
                "-s", cacheStrategy,
                "-c", cacheSize,
                "-d", dataDir,
                "-l", String.format("logs/server_%d.log", port),
                "-r", replicationFactor
        ).map(String::valueOf)
                .forEachOrdered(parameters::add);
        return parameters;
    }

    public void addServer() {
        final Future<?> server = startServer(getServerParameters());
        servers.add(server);
        this.addresses.add(String.format("127.0.0.1 %d", port));
        port++;
    }

    private Future<?> startServer(List<String> parameters) {
        LOGGER.trace("Launching server with the config: {}", parameters);
        final String[] cliArgs = parameters.toArray(String[]::new);
        if (this.useChord) return EXECUTOR_SERVICE.submit(wrapWarnLogging(() -> MainChord.main(cliArgs)));
        else return EXECUTOR_SERVICE.submit(wrapWarnLogging(() -> Main.main(cliArgs)));
    }

    public void stopServer() {
        LOGGER.trace("Stopping server");

        if (this.servers.size() <= 1) {
            return;
        }

        int index = RANDOM.nextInt(this.servers.size());

        servers.get(index).cancel(true);
        servers.remove(index);
        addresses.remove(index);
    }

}
