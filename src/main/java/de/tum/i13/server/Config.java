package de.tum.i13.server;

import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.shared.BaseConfig;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class Config extends BaseConfig {

    private static final Logger LOGGER = LogManager.getLogger(Config.class);

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
            "port and ip, e.g., 192.168.1.1:5153. Default: ${DEFAULT-VALUE}")
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
    @CommandLine.Option(names = "-ll", description = "Log level. Default: ${DEFAULT-VALUE}. Valid values: ${COMPLETION-CANDIDATES}", defaultValue = "ALL", converter = LogLevelConverter.class)
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

        parseConfig(cmd, args);
        ensureDataDirectoryExistence(cfg.dataDir, cmd.getOut(), cmd.getErr());

        return cfg;
    }

    private static void ensureDataDirectoryExistence(Path dataDir, PrintWriter out, PrintWriter err) {
        if (!Files.exists(dataDir)) {
            try {
                Files.createDirectories(dataDir);
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

    private static class LogLevelConverter implements CommandLine.ITypeConverter<StandardLevel> {

        @Override
        public StandardLevel convert(String levelString) {
            Preconditions.notNull(levelString, "Log level must not be null");
            final String capitalizedLevelString = levelString.toUpperCase();
            try {
                return StandardLevel.valueOf(capitalizedLevelString);
            } catch (IllegalArgumentException ex) {
                LOGGER.warn("Could not convert '{}' to standard log4j log level", levelString);
                return switch (capitalizedLevelString) {
                    case "ALL", "FINEST" -> StandardLevel.TRACE;
                    case "FINER", "FINE" -> StandardLevel.DEBUG;
                    case "CONFIG", "INFO" -> StandardLevel.INFO;
                    case "WARNING" -> StandardLevel.WARN;
                    case "SEVERE" -> StandardLevel.ERROR;
                    case "OFF" -> StandardLevel.OFF;
                    default -> {
                        LOGGER.warn("Could not match '{}' to standard util log level", levelString);
                        yield StandardLevel.ALL;
                    }
                };
            }
        }

    }


}
