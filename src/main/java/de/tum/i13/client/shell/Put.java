package de.tum.i13.client.shell;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.PUT_COMMAND,
        description = "Inserts a key-value pair into the storage server data structures. " +
                "Updates (overwrites) the current value with the given value if the server already contains the " +
                "specified key. Deletes the entry for the given key if <value> equals null or is not present.",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Put implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Put.class);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    @CommandLine.Parameters(
            index = "0",
            description = "arbitrary String (max length 20 Bytes)"
    )
    private String key;

    @CommandLine.Parameters(
            index = "1",
            description = "arbitrary String (max. length 120 KByte). Default: ${DEFAULT-VALUE}",
            defaultValue = CommandLine.Parameters.NULL_VALUE,
            arity = "0..1"
    )
    private String value;

    @Override
    public Integer call() throws PutException {
        LOGGER.info("Trying to put to key '{}' value '{}'", key, value);

        if ("null".equals(value)) value = null;

        final KVMessage storageResponse = parent.remoteStorage.put(key, value);
        final KVMessage.StatusType storageStatus = storageResponse.getStatus();
        final PrintWriter out = commandSpec.commandLine().getOut();
        if (storageStatus == KVMessage.StatusType.PUT_SUCCESS) {
            LOGGER.info("Remote storage successfully put key '{}' to value '{}'", key, value);
            out.printf("Successfully put value '%s' for key '%s'%n", value, key);
            return ExitCode.SUCCESS.getValue();
        } else if (storageStatus == KVMessage.StatusType.PUT_UPDATE) {
            LOGGER.info("Remote storage successfully updated key '{}' to value '{}'", key, value);
            out.printf("Successfully put value '%s' for key '%s' via update%n", value, key);
            return ExitCode.SUCCESS.getValue();
        } else if (storageStatus == KVMessage.StatusType.PUT_ERROR) {
            LOGGER.info("Remote storage returned error while putting key '{}' to value '{}'", key, value);
            out.printf("Could not put key '%s' to value '%s' on remote storage%n", key, value);
            return ExitCode.STORAGE_ERROR.getValue();
        } else if (value == null && storageStatus == KVMessage.StatusType.DELETE_SUCCESS) {
            LOGGER.info("Remote storage successfully deleted key '{}'", key);
            out.printf("Successfully deleted key '%s'%n", key);
            return ExitCode.SUCCESS.getValue();
        } else if (value == null && storageStatus == KVMessage.StatusType.DELETE_ERROR) {
            LOGGER.info("Remote storage returned error while deleting key '{}'", key);
            out.printf("Could not delete key '%s' on remote storage%n", key);
            return ExitCode.STORAGE_ERROR.getValue();
        } else if (storageStatus == KVMessage.StatusType.UNDEFINED) {
            LOGGER.warn("Remote storage returned error while putting key '{}' with value '{}' with message: {}",
                    () -> key, () -> value, storageResponse::toString);
            out.printf("Remote storage returned an error with message: %s", storageResponse);
            return ExitCode.STORAGE_ERROR.getValue();
        } else {
            final PutException putException = new PutException(
                    "Remote storage returned unprocessable status code %s while putting key '%s'",
                    storageStatus,
                    key
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, putException);
            throw putException;
        }
    }

}
