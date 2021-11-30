package de.tum.i13.client.shell;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.NetworkConnection;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.QUIT_COMMAND,
        description = "Closes the interface",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Quit implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Quit.class);

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    /**
     * Quits the shell.
     * Close any existing connection before quitting the program
     *
     * @throws ClientException in case the quitting process is unsuccessful
     */
    @Override
    public Integer call() throws ClientException {
        final NetworkConnection networkConnection = parent.remoteStorage;
        if (networkConnection.isConnected()) {
            LOGGER.info("Disconnecting from '{}:{}'", networkConnection.getAddress(), networkConnection.getPort());
            commandSpec.commandLine().getOut().println("Disconnecting from server");
            networkConnection.disconnect();
        }

        LOGGER.info("Quitting application.");
        commandSpec.commandLine().getOut().println("Application exit!");
        return ExitCode.QUIT_PROGRAMM.getValue();
    }

}
