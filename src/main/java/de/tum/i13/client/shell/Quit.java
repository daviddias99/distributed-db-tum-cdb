package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.QUIT_COMMAND,
        description = "Closes the interface",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Quit implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Quit.class);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    /**
     * Quits the shell.
     * Close any existing connection before quitting the program
     *
     * @throws ClientException in case the quitting process is unsuccessful
     */
    @Override
    public Integer call() throws ClientException {
        if (parent.client.isConnected()) {
            LOGGER.info("Disconnecting from {}:{}", parent.address, parent.port);
            parent.client.disconnect();
        }

        LOGGER.info("Quitting application.");
        commandSpec.commandLine().getOut().println("Application exit!");
        return ExitCode.QUIT_PROGRAMM.getValue();
    }

}
