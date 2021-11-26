package de.tum.i13.client.shell;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.RemotePersistentStorage;
import de.tum.i13.server.kv.KVException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

/**
 * Handles {@link KVException} thrown by the {@link RemotePersistentStorage}
 */
class ExecutionExceptionHandler implements CommandLine.IExecutionExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger(ExecutionExceptionHandler.class);

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine,
                                        CommandLine.ParseResult parseResult) throws Exception {
        if (ex instanceof KVException || ex instanceof ClientException) {
            LOGGER.atError()
                    .withThrowable(ex)
                    .log("Caught {}", ex.getClass().getSimpleName());
            final String exceptionLocation = ex instanceof KVException ? "remote storage" : "network connection";
            commandLine.getOut().printf("Error in %s: %s%n", exceptionLocation, ex.getMessage());
            return (ex instanceof KVException ? ExitCode.STORAGE_EXCEPTION : ExitCode.CLIENT_EXCEPTION).getValue();
        } else {
            LOGGER.fatal("Caught unexpected exception. Rethrowing the exception.", ex);
            commandLine.getOut().println("An unexpected error occurred. " +
                    "Please contact the administrator or consult the logs");
            throw ex;
        }
    }

}
