package de.tum.i13.client.shell;

import de.tum.i13.client.exceptions.ClientException;
import picocli.CommandLine;

/**
 * Handles a {@link ClientException}
 */
class ClientExceptionHandler implements CommandLine.IExecutionExceptionHandler {

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine,
                                        CommandLine.ParseResult parseResult) throws Exception {
        if (ex instanceof ClientException) {
            final ClientException exception = (ClientException) ex;
            Shell.LOGGER.error("Exception type: {}. Exception reason: {}", exception.getType(), exception.getReason());
            commandLine.getOut().printf("Error: %s%n", exception.getReason());
            return 0;
        } else {
            Shell.LOGGER.fatal("Caught unexpected exception", ex);
            throw ex;
        }
    }

}
