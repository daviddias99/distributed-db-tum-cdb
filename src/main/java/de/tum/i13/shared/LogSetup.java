package de.tum.i13.shared;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.spi.StandardLevel;

import java.nio.file.Path;

/**
 * Sets up the logging in the application
 */
public class LogSetup {

    private LogSetup() {}

    /**
     * Set up the logging of the application.
     *
     * Configure the path of the logging file with a system property.
     * The default value is {@code logs/cdb.log} in the current directory.
     *
     * Additionally configure the log level of the root logger.
     *
     * @param logfile the path of the file where to log to
     * @param logLevel the log level to use
     */
    public static void setupLogging(Path logfile, Level logLevel) {
        System.setProperty("logFilename", logfile.toString());
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();

        Configurator.setRootLevel(logLevel);
    }

    /**
     * Overloads {@link #setupLogging(Path, Level)} with {@link Level#getLevel(String)}
     * to convert {@link StandardLevel} to {@link Level}
     *
     * @param logFile the path of the file where to log to
     * @param logLevel the log level to use
     * @see #setupLogging(Path, Level)
     */
    public static void setupLogging(Path logFile, StandardLevel logLevel) {
        setupLogging(logFile, Level.getLevel(logLevel.toString()));
    }
}