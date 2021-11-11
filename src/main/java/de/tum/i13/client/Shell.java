package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.shell.jline3.PicocliCommands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.Callable;

public class Shell {

    private static final Logger LOGGER = LogManager.getLogger(Shell.class.getName());
    private boolean quit;

    /**
     * Creates a new command line interface.
     */
    public Shell() {
        quit = false;
    }

    /**
     * Main method. Creates a shell and starts it.
     */
    public static void main(String[] args) {
        final int exitCode = new CommandLine(new CLICommands()).execute("help");
        System.exit(exitCode);
    }

    /**
     * Starts shell. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to {@code <address>:<port>} , disconnect, send a message to the server, change logging level.
     */
    public void start() throws IOException {
        BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
        quit = false;
        while (!quit) {
            //print prompt
            System.out.print(Constants.PROMPT);

            //read user input from console
            String input = cons.readLine();
            String[] tokens = input.trim().split("\\s+");

            try {
                handleInput(input, tokens);
            } catch (ClientException e) {
                handleClientException(e);
            }
        }
    }

    private void handleInput(String input, String[] tokens) throws ClientException {
//        if (input.isEmpty()) {
//            handleFaultyCommand("Commands should not be empty");
//        }
//        else if (Character.isWhitespace(input.charAt(0))) {
//            handleFaultyCommand("Commands should not start with whitespace");
//        }
//        //connect command should be in format: "connect <address> <port>"
//        else if (tokens.length == 3 && tokens[0].equals(Constants.CONNECT_COMMAND)) {
//            connect(tokens);
//        }
//        //disconnect command should provide status report upon successful disconnection
//        else if (tokens.length == 1 && tokens[0].equals(Constants.DISCONNECT_COMMAND)) {
//            disconnect();
//        }
//        //command to send message to server: "send <message>"
//        else if (tokens[0].equals(Constants.SEND_COMMAND)) {
//            send(input);
//        }
//        //command to change the logging level: "logLevel <level>"
//        else if (tokens.length == 2 && tokens[0].equals(Constants.LOG_COMMAND)) {
//            changeLogLevel(tokens[1]);
//        }
//        //help command to print information about the program
//        else if (tokens.length == 1 && tokens[0].equals(Constants.HELP_COMMAND)) {
//            printHelp();
//        }
//        //quit command should close any existing connection before quitting the program
//        else if (tokens.length == 1 && tokens[0].equals(Constants.QUIT_COMMAND)) {
//            quit();
//        }
//        //unrecognized input
//        else {
//            handleFaultyCommand("Unrecognized command.");
//        }
    }

    @Command(name = "",
            description = "CLI application for interacting with a server on the client side",
            mixinStandardHelpOptions = true,
            subcommands = {PicocliCommands.ClearScreen.class, CommandLine.HelpCommand.class,
                    Disconnect.class, Send.class, Connect.class, ChangeLogLevel.class, Quit.class
    })
    private static class CLICommands implements Runnable {


        private final EchoClient client;
        private final boolean quit;
        private final PrintWriter out;
        /**
         * Server address of current connection
         */
        private String address;
        /**
         * Port number of current connection
         */
        private int port;                    //

        public CLICommands() {
            client = new EchoClient();
            quit = false;
            out = new PrintWriter(System.out);
        }

        @Override
        public void run() {
            out.println("hello");
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
         * Disconnects from the server
         *
         * @throws ClientException in case the disconnect is unsuccessful
         */
        @Override
        public Void call() throws ClientException {
            LOGGER.info("Disconnecting from {}:{}", parent.address, parent.port);
            parent.client.disconnect();
            parent.out.println(Constants.PROMPT + "Successfully disconnected.");
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
        /**
         * Input to send
         */
        @Parameters(
                index = "0",
                description = "The input to send"
        )
        private String input;

        /**
         * Send the input to the server
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
            parent.out.println(Constants.PROMPT + responseStr);
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
                index = "0",
                description = "The port to of the server to connect to"
        )
        private int port;

        @Parameters(
                index = "1",
                description = "The address of the server to connect to"
        )
        private String address;

        /**
         * Initiates connection of client to server using the EchoClient instance if the provided
         * arguments are in the correct format.
         *
         * @throws ClientException       if the connection is unsuccessful
         */
        @Override
        public Integer call() throws ClientException {
            //create new connection and receive confirmation from server
            LOGGER.info("Initiating connection to {}:{}", address, port);
            byte[] response = parent.client.connectAndReceive(address, port);
            String confirmation = new String(response, 0, response.length - 2, Constants.TELNET_ENCODING);
            System.out.println(Constants.PROMPT + confirmation);
            LOGGER.info("Connection to {}:{} successful.", address, port);
            return 0;
            // TODO Handle faulty command
//            handleFaultyCommand("Unrecognized command. Port number in wrong format.");
        }

    }

    @Command(
            name = Constants.LOG_COMMAND,
            // TODO Add possible values Level.values()
            description = "changes the logging level to the <new_level>. Possible values: ",
            mixinStandardHelpOptions = true,
            subcommands = CommandLine.HelpCommand.class
    )
    private static class ChangeLogLevel implements Callable<Integer> {

        @ParentCommand
        private CLICommands parent;


        // TODO check if index necessary
        // TODO Check if converter necessary
        @Parameters(
                index = "0",
                converter = LogLevelConverter.class,
                description = "The desired log level"
        )
        private Level logLevel;


        /**
         * Changes the logger to the specified level if valid.
         */
        @Override
        public Integer call() throws ClientException {
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
                return Level.valueOf(value);
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
         * Quits the shell
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
            parent.out.println(Constants.PROMPT + "Application exit!");
            return 0;
        }

    }

    // TODO Handle faulty commands
//    /**
//     * Handles a faulty command
//     *
//     * @param reason the reasons why the command is faulty
//     */
//    private void handleFaultyCommand(String reason) {
//        LOGGER.info(reason);
//        System.out.println(reason);
//        printHelp();
//    }

    /**
     * Handles a {@link ClientException}
     *
     * @param e the exception to handle
     */
    private void handleClientException(ClientException e) {
        LOGGER.error("Exception type: {}. Exception reason: {}", e.getType(), e.getReason());
        //TODO Figure out which error message to print to console & when
        System.out.println(Constants.PROMPT + "Error: " + e.getReason());
    }

}