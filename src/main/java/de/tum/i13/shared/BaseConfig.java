package de.tum.i13.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.PrintWriter;

/**
 * Basic configuration that can be extended in different ways, for example for the server or the client.
 *
 * @see de.tum.i13.server.Config
 */
public class BaseConfig {

    private static final Logger LOGGER = LogManager.getLogger(BaseConfig.class);

    /**
     * Usage help
     */
    @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
    public boolean usageHelp;

    /**
     * Parses the config using the supplied command line.
     * The type of config is set when creating the command line. That's why there is no type parameter.
     * The options and parameters are set via side effects in this function. That's why there is no return value.
     *
     * @param cmd  the command line with which to parse the config
     * @param args the arguments which to parse into a config
     */
    public static void parseConfig(CommandLine cmd, String[] args) {
        LOGGER.trace("Parsing command line arguments");

        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException ex) {
            final CommandLine exCmd = ex.getCommandLine();
            ex.printStackTrace(exCmd.getErr());
            exCmd.usage(exCmd.getOut());
            System.exit(-1);
        }

        showHelpAndExitIfRequested(cmd, cmd.getOut());
    }

    private static void showHelpAndExitIfRequested(CommandLine cmd, PrintWriter out) {
        if (cmd.isUsageHelpRequested()) {
            cmd.usage(out);
            System.exit(0);
        }
    }

}