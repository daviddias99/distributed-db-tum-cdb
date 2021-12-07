package de.tum.i13.client.shell;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = Constants.DISCONNECT_COMMAND,
        description = "Disconnects from an existing connection",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Disconnect implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Disconnect.class);

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    /**
     * Disconnects from the server.
     * Provides status report upon successful disconnection
     *
     * @throws CommunicationClientException in case the disconnect is unsuccessful
     */
    @Override
    public Integer call() throws CommunicationClientException {
        final NetworkConnection networkConnection = parent.remoteStorage;
        LOGGER.info("Disconnecting from '{}:{}'", networkConnection.getAddress(), networkConnection.getPort());
        networkConnection.disconnect();
        commandSpec.commandLine().getOut().println("Successfully disconnected.");
        return ExitCode.SUCCESS.getValue();
    }

}
