package de.tum.i13.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import java.nio.file.Path;

/**
 * Sets up the logging in the application
 */
public class LogSetup {

    private LogSetup() {}

    /**
     * Configure the path of the logging file with a system property.
     * The default value is {@code logs/cdb.log} in the current directory.
     *
     * @param logfile the path of the file where to log to
     */
    public static void setupLogging(Path logfile) {
        System.setProperty("logFilename", logfile.toString());
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();
    }
}