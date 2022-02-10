package de.tum.i13.shared;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.spi.StandardLevel;

import java.nio.file.Path;

/**
 * Sets up the logging in the application
 */
public class LogSetup {

    private static final Logger LOGGER = LogManager.getLogger(LogSetup.class);

    private LogSetup() {
    }

    /**
     * Set up the logging of the application.
     * <p>
     * Configure the path of the logging file with a system property.
     * The default value is {@code logs/cdb.log} in the current directory.
     * <p>
     * Additionally configure the log level of the root logger.
     *
     * @param logfile  the path of the file where to log to
     * @param logLevel the log level to use
     */
    public static void setupLogging(Path logfile, Level logLevel) {
        setLogFile(logfile);

        setRootLoggerLevel(logLevel);
        LOGGER.info("Setup logging to file '{}' and level '{}'", logfile, logLevel);
    }

    public static void setLogFile(Path logfile) {
        ThreadContext.put("ROUTINGKEY", logfile.toString());
    }

    /**
     * Overloads {@link #setupLogging(Path, Level)} with {@link Level#getLevel(String)}
     * to convert {@link StandardLevel} to {@link Level}
     *
     * @param logFile  the path of the file where to log to
     * @param logLevel the log level to use
     * @see #setupLogging(Path, Level)
     */
    public static void setupLogging(Path logFile, StandardLevel logLevel) {
        setupLogging(logFile, Level.getLevel(logLevel.toString()));
    }

    /**
     * Sets the level of the root logger and propagates the changes to descending logger.
     *
     * @param level the {@link Level} to set the logger to
     * @return the previously set {@link Level} of the logger
     */
    public static String setRootLoggerLevel(Level level) {
        final LoggerContext ctx = getLoggerContext();
        final LoggerConfig rootLoggerConfig = ctx.getConfiguration().getRootLogger();
        final String oldLevelName = ctx.getRootLogger().getLevel().name();

        rootLoggerConfig.setLevel(level);
        ctx.updateLoggers();
        return oldLevelName;
    }

    private static LoggerContext getLoggerContext() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return (LoggerContext) LogManager.getContext(classLoader, false);
    }

}