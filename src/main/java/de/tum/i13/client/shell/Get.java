package de.tum.i13.client.shell;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.GetException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = Constants.GET_COMMAND,
        description = "Retrieves the value for the given key from the storage server",
        mixinStandardHelpOptions = true,
        subcommands = CommandLine.HelpCommand.class
)
class Get implements Callable<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(Get.class);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @CommandLine.ParentCommand
    private CLICommands parent;

    @CommandLine.Parameters(
            index = "0",
            description = "the key that indexes the desired value (max length 20 Bytes)"
    )
    private String key;

    @Override
    public Integer call() throws GetException {
        LOGGER.info("Trying to get key '{}'", key);

        final KVMessage storageResponse = parent.remoteStorage.get(key);
        final KVMessage.StatusType storageStatus = storageResponse.getStatus();
        final PrintWriter out = commandSpec.commandLine().getOut();
        if (storageStatus == KVMessage.StatusType.GET_SUCCESS) {
            final String value = storageResponse.getValue();
            LOGGER.info("Remote storage returned for key '{}' value '{}'", key, value);
            out.printf("Retrieved value '%s' for key '%s'%n", value, key);
            return ExitCode.SUCCESS.getValue();
        } else if (storageStatus == KVMessage.StatusType.GET_ERROR) {
            LOGGER.info("Remote storage returned error while getting key '{}'", key);
            out.printf("Could not retrieve key '%s' from remote storage%n", key);
            return ExitCode.STORAGE_ERROR.getValue();
        } else if (storageStatus == KVMessage.StatusType.ERROR) {
            LOGGER.warn("Remote storage returned error while getting key '{}' with message: {}", () -> key,
                    storageResponse::toString);
            out.printf("Remote storage returned an error with message: %s", storageResponse);
            return ExitCode.STORAGE_ERROR.getValue();
        } else {
            final GetException getException = new GetException(
                    "Remote storage returned unprocessable status code %s while getting key %s",
                    storageStatus,
                    key
            );
            LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, getException);
            throw getException;
        }
    }

}
