package de.tum.i13.client.shell;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.spi.StandardLevel;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.LOG_COMMAND,
        description = "Changes the logging level",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class ChangeLogLevel implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(ChangeLogLevel.class);

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.Parameters(
            index = "0",
            description = "The desired log level. Valid values: ${COMPLETION-CANDIDATES}"
    )
    private StandardLevel logLevel;

    /**
     * Changes the logger to the specified level if valid.
     */
    @Override
    public Integer call() {
        final Level level = Level.valueOf(logLevel.toString());
        final String newLevelName = level.name();

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(classLoader, false);
        final LoggerConfig rootLoggerConfig = ctx.getConfiguration().getRootLogger();
        final String oldLevelName = ctx.getRootLogger().getLevel().name();

        rootLoggerConfig.setLevel(level);
        ctx.updateLoggers();

        LOGGER.info("Log level set from {} to {}.", oldLevelName, newLevelName);
        commandSpec.commandLine().getOut().printf("Log level set from %s to %s.%n", oldLevelName, newLevelName);

        return ExitCode.SUCCESS.getValue();
    }

}
