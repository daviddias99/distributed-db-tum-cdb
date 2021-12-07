package de.tum.i13.client.shell;

import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.NetworkMessageServer;
import de.tum.i13.shared.persistentstorage.DistributedPersistentStorage;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
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
        remoteStorage = new DistributedPersistentStorage(client);
    }

}
