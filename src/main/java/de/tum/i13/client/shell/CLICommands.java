package de.tum.i13.client.shell;

import de.tum.i13.client.EchoClient;
import picocli.CommandLine;

@CommandLine.Command(name = "",
        description = "CLI application for interacting with a server on the client side",
        mixinStandardHelpOptions = true,
        subcommands = {CommandLine.HelpCommand.class, Connect.class, Disconnect.class, Quit.class,
                Send.class, ChangeLogLevel.class
        })
class CLICommands {

    final EchoClient client;

    /**
     * Server address of current connection
     */
    String address;
    /**
     * Port number of current connection
     */
    int port;

    CLICommands() {
        client = new EchoClient();
    }

}
