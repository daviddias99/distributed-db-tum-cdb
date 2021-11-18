package de.tum.i13.client.shell;

import de.tum.i13.client.EchoClient;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

import java.io.PrintWriter;

@CommandLine.Command(name = "",
        description = "CLI application for interacting with a server on the client side",
        mixinStandardHelpOptions = true,
        subcommands = {PicocliCommands.ClearScreen.class, CommandLine.HelpCommand.class,
                Disconnect.class, Send.class, Connect.class, ChangeLogLevel.class,
                Quit.class
        })
class CLICommands {

    final EchoClient client;
    PrintWriter out;
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

    void setWriter(PrintWriter writer) {
        out = writer;
    }

}
