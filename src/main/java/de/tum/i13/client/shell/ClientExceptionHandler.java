package de.tum.i13.client.shell;

import de.tum.i13.client.net.ClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

/**
 * Handles a {@link ClientException}
 */
class ClientExceptionHandler implements CommandLine.IExecutionExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger(ClientExceptionHandler.class);

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine,
                                        CommandLine.ParseResult parseResult) throws Exception {
        if (ex instanceof ClientException) {
            final ClientException exception = (ClientException) ex;
            LOGGER.error("Exception type: {}. Exception reason: {}", exception.getType(), exception.getReason());
            commandLine.getOut().printf("Error: %s%n", exception.getReason());
            return ExitCode.CLIENT_EXCEPTION.getValue();
        } else {
            LOGGER.fatal("Caught unexpected exception. Rethrowing the exception.", ex);
            commandLine.getOut().println("An unexpected error occurred" +
                    ". Please contact the administrator or consult the logs");
            throw ex;
        }
    }

}
