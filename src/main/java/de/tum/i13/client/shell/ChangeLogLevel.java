package de.tum.i13.client.shell;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.jline.utils.Log;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(
        name = Constants.LOG_COMMAND,
        description = "Changes the logging level",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class ChangeLogLevel implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(ChangeLogLevel.class);


    @CommandLine.ParentCommand
    private CLICommands parent;

    @CommandLine.Parameters(
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
        Stream.of(ChangeLogLevel.class, CLICommands.class, ClientExceptionHandler.class, Connect.class,
                Disconnect.class, Quit.class, Send.class, Shell.class)
                .map(LogManager::getLogger)
                .forEach(logger -> Configurator.setLevel(logger.getName(), logLevel));

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
                throw new CommandLine.TypeConversionException(
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
