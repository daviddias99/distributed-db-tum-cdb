package de.tum.i13.server;

import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class Config {

    @CommandLine.Option(
            names = "-p",
            description = "sets the port of the server. Default: ${DEFAULT-VALUE}",
            defaultValue = "5153"
    )
    public int port;

    @CommandLine.Option(
            names = "-a",
            description = "which address the server should listen to. Default: ${DEFAULT-VALUE}",
            defaultValue = "127.0.0.1")

    public String listenaddr;

    @CommandLine.Option(
            names = "-b",
            description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, " +
                    "port and ip, e.g., 192.168.1.1:5153. Default: ${DEFAULT-VALUE}",
            defaultValue = "clouddatabases.i13.in.tum.de:5153"
    )
    public InetSocketAddress bootstrap;

    @CommandLine.Option(
            names = "-d",
            description = "Directory for files. Default: ${DEFAULT-VALUE}",
            defaultValue = "data/"
    )
    public Path dataDir;

    @CommandLine.Option(
            names = "-l",
            description = "Logfile. Default: ${DEFAULT-VALUE}",
            defaultValue = "logs/server.log"
    )
    public Path logfile;

    @CommandLine.Option(
            names = "-h",
            description = "Displays help",
            usageHelp = true
    )
    public boolean usageHelp;

    public static Config parseCommandlineArgs(String[] args) {
        Config cfg = new Config();
        final CommandLine cmd = new CommandLine(cfg)
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
                ", listenaddr='" + listenaddr + "'" +
                ", bootstrap=" + bootstrap +
                ", dataDir=" + dataDir +
                ", logfile=" + logfile +
                ", usagehelp=" + usageHelp +
                '}';
    }

}

