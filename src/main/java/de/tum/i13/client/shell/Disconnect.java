package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = Constants.DISCONNECT_COMMAND,
        description = "Disconnects from an existing connection",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Disconnect implements Callable<Void> {

    private static final Logger LOGGER = LogManager.getLogger(Disconnect.class);

    @CommandLine.ParentCommand
    private CLICommands parent;

    /**
     * Disconnects from the server.
     * Provides status report upon successful disconnection
     *
     * @throws ClientException in case the disconnect is unsuccessful
     */
    @Override
    public Void call() throws ClientException {
        LOGGER.info("Disconnecting from {}:{}", parent.address, parent.port);
        parent.client.disconnect();
        parent.out.println("Successfully disconnected.");
        return null;
    }

}
