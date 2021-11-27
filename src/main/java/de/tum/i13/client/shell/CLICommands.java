package de.tum.i13.client.shell;

import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.client.net.NetworkMessageServer;
import de.tum.i13.client.net.NetworkPersistentStorage;
import de.tum.i13.client.net.WrappingPersistentStorage;
import picocli.CommandLine;

@CommandLine.Command(name = "",
        description = "CLI application for interacting with a server on the client side",
        mixinStandardHelpOptions = true,
        subcommands = {CommandLine.HelpCommand.class, Connect.class, Disconnect.class, Quit.class,
                Get.class, Put.class, ChangeLogLevel.class
        })
class CLICommands {

    final NetworkPersistentStorage remoteStorage;

    CLICommands() {
        final NetworkMessageServer client = new CommunicationClient();
        remoteStorage = new WrappingPersistentStorage(client);
    }

}
