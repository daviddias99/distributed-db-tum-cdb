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
    String ecsAddress = "127.0.0.1:25670";

    int cacheSize;
    CachingStrategy cacheStrategy;
    int bTreeNodeSize;
    int replicationFactor;
    boolean useChord;

    ServerManager(ExperimentConfiguration experimentConfiguration) {
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

        this.addServerHook();
    }

    private String getServerCommand() {
        String dataDir = Paths.get("data", Integer.toString(port)).toString();
        Random rand = new Random();
        String bootstrap = "";

        if(this.useChord) {
            if (!this.addresses.isEmpty()) {
                String address = this.addresses.get(rand.nextInt(this.addresses.size()));
                bootstrap = String.format(" -b %s", String.join(":",address.split(" ")) ); 
            }
        } else {
            bootstrap = String.format(" -b %s", ecsAddress);
        }

        String jar = String.format("target/kv-server%s.jar", this.useChord ? "-chord" : "");

        return String.format("java -jar %s%s -ll TRACE -t %d -p %d -s %s -c %d -d " +
                        "%s -l logs/server_%d.log -r %d", jar, bootstrap, this.bTreeNodeSize, this.port,
                this.cacheStrategy, this.cacheSize, dataDir, this.port, this.replicationFactor);
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
            String commString = this.getServerCommand();
            LOGGER.trace("Launching server: {}", commString);
            System.out.println(String.format("Launching server: %s", commString));
            ProcessBuilder processBuilder = new ProcessBuilder(commString.split(" "));
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
