package de.tum.i13.client.shell;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.CONNECT_COMMAND,
        description = "Establishes a connection to a server",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Connect implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Connect.class);

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    @CommandLine.Parameters(
            index = "0",
            description = "The address of the server to connect to"
    )
    private String address;

    private int port;

    @CommandLine.Parameters(
            index = "1",
            description = "The port to of the server to connect to. " +
                    "Must be an integer between 0 and 65535 inclusive"
    )
    private void setPort(int value) {
        if (value >= 0 && value <= 65535) {
            this.port = value;
        } else {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    String.format(
                            "Invalid value '%s' for parameter <port>. " +
                                    "Port number must be between 0 and 65535 inclusive",
                            value
                    ));
        }
    }


    /**
     * Initiates connection of client to server using the EchoClient instance if the provided
     * arguments are in the correct format.
     *
     * @throws CommunicationClientException if the connection is unsuccessful
     */
    @Override
    public Integer call() throws CommunicationClientException {
        //create new connection and receive confirmation from server
        LOGGER.info("Initiating connection to '{}:{}'", address, port);

        String confirmation = parent.remoteStorage.connectAndReceive(address, port);
        commandSpec.commandLine().getOut().println(confirmation);
        LOGGER.info("Connection to '{}:{}' successful.", address, port);
        return ExitCode.SUCCESS.getValue();
    }

}
