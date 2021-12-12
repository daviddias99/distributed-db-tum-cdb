package de.tum.i13.server;

import de.tum.i13.server.cache.CachingStrategy;
import org.apache.logging.log4j.spi.StandardLevel;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Server configuration. Provides function to parse command line arguments
 */
public class Config {
    /**
     * Port where the server listens to commands
     */
    @CommandLine.Option(names = "-p", description = "sets the port of the server. Default: ${DEFAULT-VALUE}", defaultValue = "5153")
    public int port;

    /**
     * Address where the server listens to commands
     */
    @CommandLine.Option(names = "-a", description = "which address the server should listen to. Default: ${DEFAULT-VALUE}", defaultValue = "127.0.0.1")
    public String listenAddress;

    /**
     * address:port of the ECS server
     */
    @CommandLine.Option(names = "-b", description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, "
            +
            "port and ip, e.g., 192.168.1.1:5153. Default: ${DEFAULT-VALUE}", defaultValue = "clouddatabases.i13.in.tum.de:5153")
    public InetSocketAddress bootstrap;

    /**
     * Directory for persistent storage
     */
    @CommandLine.Option(names = "-d", description = "Directory for files. Default: ${DEFAULT-VALUE}", defaultValue = "data/")
    public Path dataDir;

    /**
     * Directory for logs
     */
    @CommandLine.Option(names = "-l", description = "Logfile. Default: ${DEFAULT-VALUE}", defaultValue = "logs/server.log")
    public Path logfile;

    /**
     * Log leve
     */
    @CommandLine.Option(names = "-ll", description = "Log level. Default: ${DEFAULT-VALUE}. Valid values: ${COMPLETION-CANDIDATES}", defaultValue = "ALL")
    public StandardLevel logLevel;

    /**
     * Size in elements of the cache
     */
    @CommandLine.Option(names = "-c", description = "Size of the cache, e.g., 100 keys. Default: ${DEFAULT-VALUE}", defaultValue = "100")
    public int cacheSize;

    /**
     * Caching strategy
     */
    @CommandLine.Option(names = "-s", description = "Cache displacement strategy. Default: ${DEFAULT-VALUE}", defaultValue = "FIFO")
    public CachingStrategy cachingStrategy;

    /**
     * Usage help
     */
    @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
    public boolean usageHelp;

    /**
     * BTree minimum degree
     */
    @CommandLine.Option(names = { "-t",
            "-bTreeMinDegree" }, description = "Minimum degree used by the BTree Persistent storage. Each BTree node stores 2*minimumDegree - 1 values.", defaultValue = "15"

    )
    public int minimumDegree;

    /**
     * Parse the command line arguments into a Config object
     * @param args command line arguments
     * @return server config
     */
    public static Config parseCommandlineArgs(String[] args) {
        Config cfg = new Config();
        final CommandLine cmd = new CommandLine(cfg)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter());
        final PrintWriter out = cmd.getOut();
        final PrintWriter err = cmd.getErr();

        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException ex) {
            final CommandLine exCmd = ex.getCommandLine();
            ex.printStackTrace(exCmd.getErr());
            exCmd.usage(exCmd.getOut());
            System.exit(-1);
        }

        showHelpAndExitIfRequested(cmd, out);
        ensureDataDirectoryExistence(cfg.dataDir, out, err);

        return cfg;
    }

    private static void showHelpAndExitIfRequested(CommandLine cmd, PrintWriter out) {
        if (cmd.isUsageHelpRequested()) {
            cmd.usage(out);
            System.exit(0);
        }
    }

    private static void ensureDataDirectoryExistence(Path dataDir, PrintWriter out, PrintWriter err) {
        if (!Files.exists(dataDir)) {
            try {
                Files.createDirectory(dataDir);
            } catch (IOException ex) {
                out.println("Could not create directory");
                ex.printStackTrace(err);
                System.exit(-1);
            }
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "port=" + port +
                ", listenAddress='" + listenAddress + "'" +
                ", bootstrap=" + bootstrap +
                ", dataDir=" + dataDir +
                ", logfile=" + logfile +
                ", btreeMinDegree=" + minimumDegree +
                ", usageHelp=" + usageHelp +
                '}';
    }

}
