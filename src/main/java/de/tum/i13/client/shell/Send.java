package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = Constants.SEND_COMMAND,
        description = "Sends a message to the server and receives a response",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Send implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Send.class);

    @CommandLine.ParentCommand
    private CLICommands parent;

    // TODO Make sure that input with spaces are possible
    @CommandLine.Parameters(
            index = "0",
            description = "The input to send"
    )
    private String input;

    /**
     * Send a message to the server
     *
     * @throws ClientException in case the sending process is unsuccessful
     */
    @Override
    public Integer call() throws ClientException {
        //send the message in bytes after appending the delimiter
        LOGGER.info("Sending message to {}:{}", parent.address, parent.port);
        parent.client.send((input.substring(5) + Constants.TERMINATING_STR).getBytes());
        receiveMessage();
        return 0;
    }

    /**
     * Called after sending a message to the server. Receives server's response in bytes, coverts it to String
     * in proper encoding and prints it to console.
     *
     * @throws ClientException if the message cannot be received
     */
    private void receiveMessage() throws ClientException {
        //receive and print server response
        LOGGER.info("Receiving message from server.");
        byte[] response = parent.client.receive();
        String responseStr = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
        parent.out.println(responseStr);
    }

}
