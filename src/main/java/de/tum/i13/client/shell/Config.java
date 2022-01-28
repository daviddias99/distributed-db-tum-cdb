package de.tum.i13.client.shell;

import de.tum.i13.shared.BaseConfig;
import picocli.CommandLine;

/**
 * Client configuration. Provides function to parse command line arguments
 */
class Config extends BaseConfig {

    /**
     * The type of the server to connect to
     */
    @CommandLine.Option(
            names = {"-t", "--server-type"},
            description = "Type of the server the client communicates with. Default: ${DEFAULT-VALUE}. Valid values: " +
                    "${COMPLETION-CANDIDATES}",
            defaultValue = "ECS"
    )
    public ServerType serverType;

    /**
     * Parse the command line arguments into a Config object
     *
     * @param args command line arguments
     * @return server config
     */
    public static Config parseCommandlineArgs(String[] args) {
        Config cfg = new Config();
        final CommandLine cmd = new CommandLine(cfg)
                .setCaseInsensitiveEnumValuesAllowed(true);
        parseConfig(cmd, args);
        return cfg;
    }

    @Override
    public String toString() {
        return "Config{" +
                ", serverType=" + serverType +
                ", usageHelp=" + usageHelp +
                '}';
    }


}
