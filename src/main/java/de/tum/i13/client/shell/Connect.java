package de.tum.i13.client.shell;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.shared.Constants;
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

    @CommandLine.Parameters(
            index = "1",
            description = "The port to of the server to connect to. Must be an integer"
    )
    private int port;


    /**
     * Initiates connection of client to server using the EchoClient instance if the provided
     * arguments are in the correct format.
     *
     * @throws ClientException if the connection is unsuccessful
     */
    @Override
    public Integer call() throws ClientException {
        //create new connection and receive confirmation from server
        LOGGER.info("Initiating connection to {}:{}", address, port);
        parent.address = address;
        parent.port = port;

        byte[] response = parent.remoteStorage.getNetworkMessageServer().connectAndReceive(address, port);
        String confirmation = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
        commandSpec.commandLine().getOut().println(confirmation);
        LOGGER.info("Connection to {}:{} successful.", address, port);
        return ExitCode.SUCCESS.getValue();
    }

}
