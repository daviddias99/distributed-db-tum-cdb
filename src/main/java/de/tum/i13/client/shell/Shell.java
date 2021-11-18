package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ShellException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import picocli.shell.jline3.PicocliCommands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

public class Shell {

    private static final Logger LOGGER = LogManager.getLogger(Shell.class);

    private Shell() {
    }

    /**
     * Starts shell. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to {@code <address>:<port>} , disconnect, send a message to the server, change logging level.
     */
    public static void main(String[] args) {
        final CLICommands commands = new CLICommands();
        final CommandLine cmd = new CommandLine(commands)
                .setParameterExceptionHandler(new ParameterExceptionHandler())
                .setExecutionExceptionHandler(new ClientExceptionHandler());

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        final PrintWriter out = cmd.getOut();
        cmd.setErr(out);

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


//    public static void asdf(String[] args) {
//        AnsiConsole.systemInstall();
//
//        final CLICommands commands = new CLICommands();
//        PicocliCommands.PicocliCommandsFactory factory = new PicocliCommands.PicocliCommandsFactory();
//        final CommandLine cmd = new CommandLine(commands, factory)
//                .setExecutionExceptionHandler(new ClientExceptionHandler());
//        final PicocliCommands picocliCommands = new PicocliCommands(cmd);
//
//        Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));
//
//        Builtins builtins = new Builtins(workDir, null, null);
//
//        Parser parser = new DefaultParser();
//        try (Terminal terminal = TerminalBuilder.terminal()) {
//            factory.setTerminal(terminal);
//
//            // Configures system registry
//            SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
//            systemRegistry.setCommandRegistries(builtins, picocliCommands);
//            systemRegistry.register(Constants.HELP_COMMAND, picocliCommands);
//
//            // Configures line reader
//            LineReader reader = LineReaderBuilder.builder()
//                    .terminal(terminal)
//                    .completer(systemRegistry.completer())
//                    .parser(parser)
//                    .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
//                    .build();
//            builtins.setLineReader(reader);
//            final PrintWriter writer = reader.getTerminal().writer();
//            commands.setWriter(writer);
//            cmd.setOut(writer)
//                    .setErr(writer);
//
//            // User input loop
//            boolean quit = false;
//            while (!quit) quit = processLine(systemRegistry, reader, writer);
//        } catch (Exception exception) {
//            LOGGER.fatal("Caught exception", exception);
//        } finally {
//            AnsiConsole.systemUninstall();
//        }
//    }
//
//    private static boolean processLine(SystemRegistry systemRegistry, LineReader reader, PrintWriter writer) {
//        boolean quit = false;
//        try {
//            systemRegistry.cleanUp();
//            String line = reader.readLine(Constants.PROMPT);
//            systemRegistry.execute(line);
//        } catch (UnknownCommandException exception) {
//            writer.println("Unknown command");
//            try {
//                systemRegistry.execute(Constants.HELP_COMMAND);
//            } catch (Exception e) {
//                final var rethrownException = new ShellException("Could not execute help command", e);
//                LOGGER.fatal(rethrownException);
//                throw rethrownException;
//            }
//        } catch (UserInterruptException | EndOfFileException e) {
//            LOGGER.info("User interrupt or end of file received. Closing application.");
//            quit = true;
//        } catch (Exception exception) {
//            systemRegistry.trace(exception);
//        }
//        return quit;
//    }

}