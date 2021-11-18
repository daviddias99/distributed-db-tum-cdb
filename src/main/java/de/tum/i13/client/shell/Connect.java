package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.CONNECT_COMMAND,
        description = "Establishes a connection to a server",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Connect implements Callable<Integer> {

    @CommandLine.ParentCommand
    private CLICommands parent;

    @CommandLine.Parameters(
            index = "1",
            description = "The port to of the server to connect to"
    )
    private int port;

    @CommandLine.Parameters(
            index = "0",
            description = "The address of the server to connect to"
    )
    private String address;

    /**
     * Initiates connection of client to server using the EchoClient instance if the provided
     * arguments are in the correct format.
     *
     * @throws ClientException if the connection is unsuccessful
     */
    @Override
    public Integer call() throws ClientException {
        //create new connection and receive confirmation from server
        Shell.LOGGER.info("Initiating connection to {}:{}", address, port);
        byte[] response = parent.client.connectAndReceive(address, port);
        String confirmation = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
        parent.out.println(confirmation);
        Shell.LOGGER.info("Connection to {}:{} successful.", address, port);
        return 0;
        // TODO Handle faulty command
//            handleFaultyCommand("Unrecognized command. Port number in wrong format.");
    }

}
