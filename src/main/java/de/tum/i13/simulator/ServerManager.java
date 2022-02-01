package de.tum.i13.simulator;

import de.tum.i13.server.cache.CachingStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Random;

public class ServerManager {

    private static final Logger LOGGER = LogManager.getLogger(ServerManager.class);

    LinkedList<Process> servers;
    LinkedList<String> addresses;
    int port = 35660;

    int cacheSize;
    CachingStrategy cacheStrategy;
    int bTreeNodeSize;

    ServerManager(ExperimentConfiguration experimentConfiguration) {
        this(
                experimentConfiguration.getStartingServerCount(),
                experimentConfiguration.getServerCacheSize(),
                experimentConfiguration.getServerCachingStrategy(),
                experimentConfiguration.getbTreeNodeSize()
        );
    }

    public ServerManager(int count, int cacheSize, CachingStrategy cacheStrategy, int bTreeNodeSize) {

        this.servers = new LinkedList<>();
        this.addresses = new LinkedList<>();

        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.bTreeNodeSize = bTreeNodeSize;

        for (int i = 0; i < count; i++) {
            this.addServer();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        this.addServerHook();
    }

    private String getServerCommand() {
        String dataDir = Paths.get("data", Integer.toString(port)).toString();
        return String.format("java -jar target/kv-server.jar -b 127.0.0.1:25670 -ll TRACE -t %d -p %d -s %s -c %d -d " +
                        "%s -l logs/server_%d.log", this.bTreeNodeSize, this.port,
                this.cacheStrategy, this.cacheSize, dataDir, this.port);
    }

    private void addServerHook() {
        Thread turnoffServersHook = new Thread(() -> {
            LOGGER.info("Turning off services");
            for (Process process : servers) {
                try {
                    Runtime.getRuntime().exec("kill -SIGINT " + process.pid());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                process.destroy();
            }
        });
        Runtime.getRuntime().addShutdownHook(turnoffServersHook);
    }

    public void addServer() {
        try {
            LOGGER.trace("Launching server");
            ProcessBuilder processBuilder = new ProcessBuilder(this.getServerCommand().split(" "));
            processBuilder.redirectOutput(Redirect.DISCARD);
            processBuilder.redirectError(Redirect.DISCARD);
            Process server = processBuilder.start();
            servers.add(server);
            this.addresses.push(String.format("127.0.0.1 %d", port));
            port++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stopServer() {
        LOGGER.trace("Stopping server");

        if (this.servers.size() <= 1) {
            return;
        }

        Random rand = new Random();
        int index = rand.nextInt(this.servers.size());
        Process server = servers.get(index);

        server.destroy();
        // try {
        //   // Process p = Runtime.getRuntime().exec("kill -SIGINT" + server.pid());
        // } catch (IOException e) {
        //   e.printStackTrace();
        // }

        servers.remove(index);
        addresses.remove(index);
    }

}
