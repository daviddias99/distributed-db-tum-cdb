package de.tum.i13.client.shell;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * Interactive shell to use on the client side for communication with the server.
 */
public class Shell {

    private static final Logger LOGGER = LogManager.getLogger(Shell.class);

    private Shell() {
    }

    /**
     * Starts shell. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to {@code <address>:<port>} , disconnect, send a message to the server, change logging level.
     *
     * @param args command line arguments of the program. Unused in this class
     */
    public static void main(String[] args) {
        final CLICommands commands = new CLICommands();
        final CommandLine cmd = new CommandLine(commands)
                .setExitCodeExceptionMapper(new ExitCodeMapper())
                .setParameterExceptionHandler(new ParameterExceptionHandler())
                .setExecutionExceptionHandler(new ExecutionExceptionHandler());

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        final PrintWriter out = cmd.getOut();

        boolean quit = false;
        while (!quit) {
            //print prompt
            out.print(Constants.PROMPT);
            out.flush();

            //read user input from console
            try {
                String line = reader.readLine();
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length >= 2 && Constants.SEND_COMMAND.equals(tokens[0])) {
                    tokens = new String[]{Constants.SEND_COMMAND, line.split("\\s", 2)[1]};
                }

                final int exitCode = cmd.execute(tokens);
                if (exitCode == ExitCode.QUIT_PROGRAMM.getValue()) {
                    quit = true;
                }
            } catch (IOException e) {
                final var rethrownException = new ShellException("Could not read line", e);
                LOGGER.fatal(rethrownException);
                throw rethrownException;
            }
        }


    }

}