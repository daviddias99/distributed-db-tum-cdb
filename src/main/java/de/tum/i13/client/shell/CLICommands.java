package de.tum.i13.client.shell;

import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.client.net.RemotePersistentStorage;
import picocli.CommandLine;

@CommandLine.Command(name = "",
        description = "CLI application for interacting with a server on the client side",
        mixinStandardHelpOptions = true,
        subcommands = {CommandLine.HelpCommand.class, Connect.class, Disconnect.class, Quit.class,
                Send.class, Get.class, Put.class, ChangeLogLevel.class
        })
class CLICommands {

    final RemotePersistentStorage storage;

    final CommunicationClient client;

    /**
     * Server address of current connection
     */
    String address;
    /**
     * Port number of current connection
     */
    int port;

    CLICommands() {
        client = new CommunicationClient();
        storage = new RemotePersistentStorage();
    }

}
