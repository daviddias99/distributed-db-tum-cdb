package de.tum.i13.client.shell;

import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.NetworkMessageServer;
import de.tum.i13.shared.persistentstorage.DistributedChordPersistentStorage;
import de.tum.i13.shared.persistentstorage.DistributedECSPersistentStorage;
import de.tum.i13.shared.persistentstorage.NetworkPersistentStorage;
import de.tum.i13.shared.persistentstorage.WrappingPersistentStorage;
import picocli.CommandLine;

/**
 * Provides the CLI commands to interact with a {@link NetworkPersistentStorage}
 */
@CommandLine.Command(name = "",
        description = "CLI application for interacting with a server on the client side",
        mixinStandardHelpOptions = true,
        subcommands = {CommandLine.HelpCommand.class, Connect.class, Disconnect.class, Quit.class,
                Get.class, Put.class, ChangeLogLevel.class
        })
public class CLICommands {

    final NetworkPersistentStorage remoteStorage;

    /**
     * Create a new instance of the CLI command with the default {@link ServerType#ECS}.
     */
    public CLICommands() {
        this(ServerType.ECS);
    }

    /**
     * Create a new instance of the CLI command with the configured {@link ServerType}
     *
     * @param serverType the {@link ServerType} which to connect to
     */
    public CLICommands(ServerType serverType) {
        final NetworkMessageServer client = new CommunicationClient();
        final NetworkPersistentStorage wrappingStorage = new WrappingPersistentStorage(client);
        remoteStorage = switch (serverType) {
            case ECS -> new DistributedECSPersistentStorage(wrappingStorage);
            case CHORD -> new DistributedChordPersistentStorage(wrappingStorage);
        };
    }

}
