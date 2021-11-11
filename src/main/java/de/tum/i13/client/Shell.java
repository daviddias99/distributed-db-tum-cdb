package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.console.impl.SystemRegistryImpl.UnknownCommandException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.TypeConversionException;
import picocli.shell.jline3.PicocliCommands;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Shell {

    private static final Logger LOGGER = LogManager.getLogger(Shell.class.getName());

    private Shell() {
    }

    /**
     * Starts shell. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to {@code <address>:<port>} , disconnect, send a message to the server, change logging level.
     */
    public static void main(String[] args) {
        AnsiConsole.systemInstall();

        final CLICommands commands = new CLICommands();
        PicocliCommands.PicocliCommandsFactory factory = new PicocliCommands.PicocliCommandsFactory();
        final CommandLine cmd = new CommandLine(commands, factory)
                .setExecutionExceptionHandler(new ClientExceptionHandler());
        final PicocliCommands picocliCommands = new PicocliCommands(cmd);

        Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));

        Builtins builtins = new Builtins(workDir, null, null);

        Parser parser = new DefaultParser();
        try (Terminal terminal = TerminalBuilder.terminal()) {
            factory.setTerminal(terminal);

            // Configures system registry
            SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
            systemRegistry.setCommandRegistries(builtins, picocliCommands);
            systemRegistry.register(Constants.HELP_COMMAND, picocliCommands);

            // Configures line reader
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(systemRegistry.completer())
                    .parser(parser)
                    .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                    .build();
            builtins.setLineReader(reader);
            final PrintWriter writer = reader.getTerminal().writer();
            commands.setWriter(writer);
            cmd.setOut(writer)
                    .setErr(writer);

            // User input loop
            boolean quit = false;
            while (!quit) quit = processLine(systemRegistry, reader, writer);
        } catch (Exception exception) {
            LOGGER.fatal("Caught exception", exception);
        } finally {
            AnsiConsole.systemUninstall();
        }
    }

    private static boolean processLine(SystemRegistry systemRegistry, LineReader reader, PrintWriter writer) {
        boolean quit = false;
        try {
            systemRegistry.cleanUp();
            String line = reader.readLine(Constants.PROMPT);
            systemRegistry.execute(line);
        } catch (UnknownCommandException exception) {
            writer.println("Unknown command");
            try {
                systemRegistry.execute(Constants.HELP_COMMAND);
            } catch (Exception e) {
                final RuntimeException rethrownException = new RuntimeException("Could not execute help command", e);
                LOGGER.fatal(rethrownException);
                throw rethrownException;
            }
        } catch (UserInterruptException | EndOfFileException e) {
            LOGGER.info("User interrupt or end of file received. Closing application.");
            quit = true;
        } catch (Exception exception) {
            systemRegistry.trace(exception);
        }
        return quit;
    }

    /**
     * Handles a {@link ClientException}
     */
    private static class ClientExceptionHandler implements CommandLine.IExecutionExceptionHandler {

        @Override
        public int handleExecutionException(Exception ex, CommandLine commandLine,
                                            CommandLine.ParseResult parseResult) throws Exception {
            if (ex instanceof ClientException) {
                final ClientException exception = (ClientException) ex;
                LOGGER.error("Exception type: {}. Exception reason: {}", exception.getType(), exception.getReason());
                commandLine.getOut().printf("Error: %s%n", exception.getReason());
                return 0;
            } else {
                LOGGER.fatal("Caught unexpected exception", ex);
                throw ex;
            }
        }

    }

    @Command(name = "",
            description = "CLI application for interacting with a server on the client side",
            mixinStandardHelpOptions = true,
            subcommands = {PicocliCommands.ClearScreen.class, CommandLine.HelpCommand.class,
                    Disconnect.class, Send.class, Connect.class, ChangeLogLevel.class, Quit.class
            })
    private static class CLICommands {

        private final EchoClient client;
        private PrintWriter out;
        /**
         * Server address of current connection
         */
        private String address;
        /**
         * Port number of current connection
         */
        private int port;

        private CLICommands() {
            client = new EchoClient();
        }

        private void setWriter(PrintWriter writer) {
            out = writer;
        }

    }

    @Command(name = Constants.DISCONNECT_COMMAND,
            description = "Disconnects from an existing connection",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class Disconnect implements Callable<Void> {

        @ParentCommand
        private CLICommands parent;

        /**
         * Disconnects from the server.
         * Provides status report upon successful disconnection
         *
         * @throws ClientException in case the disconnect is unsuccessful
         */
        @Override
        public Void call() throws ClientException {
            LOGGER.info("Disconnecting from {}:{}", parent.address, parent.port);
            parent.client.disconnect();
            parent.out.println("Successfully disconnected.");
            return null;
        }

    }

    @Command(name = Constants.SEND_COMMAND,
            description = "Sends a message to the server and receives a response",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class Send implements Callable<Integer> {

        @ParentCommand
        private CLICommands parent;

        // TODO Make sure that input with spaces are possible
        @Parameters(
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

    @Command(
            name = Constants.CONNECT_COMMAND,
            description = "Establishes a connection to a server",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class Connect implements Callable<Integer> {

        @ParentCommand
        private CLICommands parent;

        @Parameters(
                index = "1",
                description = "The port to of the server to connect to"
        )
        private int port;

        @Parameters(
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
            LOGGER.info("Initiating connection to {}:{}", address, port);
            byte[] response = parent.client.connectAndReceive(address, port);
            String confirmation = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
            parent.out.println(confirmation);
            LOGGER.info("Connection to {}:{} successful.", address, port);
            return 0;
            // TODO Handle faulty command
//            handleFaultyCommand("Unrecognized command. Port number in wrong format.");
        }

    }

    @Command(
            name = Constants.LOG_COMMAND,
            description = "Changes the logging level",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class ChangeLogLevel implements Callable<Integer> {


        @ParentCommand
        private CLICommands parent;

        @Parameters(
                index = "0",
                converter = LogLevelConverter.class,
                completionCandidates = RegisteredLogLevels.class,
                description = "The desired log level. Valid values: ${COMPLETION-CANDIDATES}"
        )
        private Level logLevel;

        /**
         * Changes the logger to the specified level if valid.
         */
        @Override
        public Integer call() {
            String oldLevelName = LOGGER.getLevel().name();
            final String newLevelName = logLevel.name();
            Configurator.setLevel(LogManager.getLogger(Shell.class).getName(), logLevel);
            LOGGER.info("Log level set from {} to {}.", oldLevelName, newLevelName);
            parent.out.printf("Log level set from %s to %s.%n", oldLevelName, newLevelName);
            return 0;
        }

        private static class LogLevelConverter implements CommandLine.ITypeConverter<Level> {

            @Override
            public Level convert(String value) {
                try {
                    return Level.valueOf(value);
                } catch (NullPointerException | IllegalArgumentException exception) {
                    LOGGER.atError()
                            .withThrowable(exception)
                            .log("Log level {} is not valid.", value);
                    throw new TypeConversionException(
                            String.format(
                                    "Log level \"%s\" is not valid. %s",
                                    value,
                                    exception.getMessage())
                    );
                }
            }

        }

        private static class RegisteredLogLevels extends ArrayList<String> {

            private RegisteredLogLevels() {
                super(Arrays.stream(Level.values())
                        .map(Level::name)
                        .collect(Collectors.toUnmodifiableList())
                );
            }

        }

    }

    @Command(
            name = Constants.QUIT_COMMAND,
            description = "Closes the interface",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class Quit implements Callable<Integer> {

        @ParentCommand
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
            parent.out.println("Application exit!");
            return 0;
        }

    }

}