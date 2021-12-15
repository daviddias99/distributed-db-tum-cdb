package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocationImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Class responsible to process commands sent by servers to the ECS.
 */
class ECSCommandProcessor implements CommandProcessor<String> {

    private static final Logger LOGGER = LogManager.getLogger(ECSCommandProcessor.class);

    public ECSCommandProcessor(){
    }

    @Override
    public String process(String command) {
        LOGGER.info("Processing command '{}'", command);
        KVMessage message = KVMessage.unpackMessage(command);
        KVMessage response = switch (message.getStatus()) {
            case SERVER_START -> processServerStart(message.getKey(), message.getValue());
            case SERVER_SHUTDOWN -> processServerShutdown(message.getKey(), message.getValue());
            default -> {
                LOGGER.error("Could not process command '{}'", command);
                yield new KVMessageImpl(StatusType.ERROR);
            }
        };

        return LOGGER.traceExit("Returning process result", response.toString());
    }

    /**
     * Method that adds a new server to {@link ExternalConfigurationService} metadata and establishes
     * a HEARTBEAT connection {@link ECSHeartbeatThread} between the ECS and the new server.
     *
     * @param address    Address of the new server to be added to the {@link ExternalConfigurationService}.
     * @param portString Port of the new server in String format.
     * @return A {@link KVMessage} indicating the status of the new server request.
     */
    private KVMessage processServerStart(String address, String portString) {
        LOGGER.debug("Processing server start at location '{}:{}'", address, portString);
        try {
            int port = Integer.parseInt(portString);
            LOGGER.trace("Starting new heartbeat thread for '{}:{}'", address, port);
            new ECSHeartbeatThread(new NetworkLocationImpl(address, port))
                    .run();
            LOGGER.trace("Adding server '{}:{}' to {}",
                    address, port, ExternalConfigurationService.class.getSimpleName());
            ExternalConfigurationService.addServer(address, port);

            return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, new KVMessageImpl(StatusType.ECS_ACK));
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to connect to {}:{}", address, portString);
        } catch (NumberFormatException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Port number not valid while trying to connect to {}:{}", address, portString);
        } catch (ECSException ex) {
            LOGGER.fatal("Could not add new server", ex);
        }
        return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, new KVMessageImpl(StatusType.ERROR));
    }

    /**
     * Method to initiate the graceful removal of a server from {@link ExternalConfigurationService}, and handoff
     * of the relevant key-value pairs to the responsible server.
     *
     * @param address    Address of the server that will shutdown.
     * @param portString Port of the server that will shut down, in String format.
     * @return A {@link KVMessage} with the status of the shutdown request.
     */
    private KVMessage processServerShutdown(String address, String portString) {
        LOGGER.debug("Processing server shutdown at '{}:{}'", address, portString);
        try {
            int port = Integer.parseInt(portString);
            LOGGER.trace("Removing server '{}:{}' and initiating handoff at {}",
                    address, port, ExternalConfigurationService.class.getSimpleName());
            ExternalConfigurationService.removeServerAndHandoffData(address, port);
            return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, new KVMessageImpl(StatusType.ECS_ACK));

        } catch (NumberFormatException ex) {
            LOGGER.fatal("Port number not valid while trying to shut down '{}:{}'", address, portString);
            return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, new KVMessageImpl(StatusType.ERROR));
        } catch (ECSException e) {
            LOGGER.fatal("An error occurred during handoff while trying to shut down '{}:{}'", address, portString);
            return LOGGER.traceExit(Constants.EXIT_LOG_MESSAGE_FORMAT, new KVMessageImpl(StatusType.ERROR));
        }
    }

}
