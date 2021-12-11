package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

/**
 * Class responsible to process commands sent by servers to the ECS.
 */
public class ECSCommandProcessor implements CommandProcessor<String> {

    private static final Logger LOGGER = LogManager.getLogger(ECSCommandProcessor.class);
    private final ExternalConfigurationService service;

    public ECSCommandProcessor(ExternalConfigurationService service) {
        this.service = service;
    }

    @Override
    public String process(String command) {
        KVMessage message = KVMessage.unpackMessage(command);
        KVMessage response = switch (message.getStatus()) {
            case SERVER_START -> start(message.getKey(), message.getValue());
            case SERVER_SHUTDOWN -> serverShutdown(message.getKey(), message.getValue());
            default -> new KVMessageImpl(StatusType.ERROR);
        };

        return response.toString();
    }

    /**
     * Method that adds a new server to {@link ExternalConfigurationService} metadata and establishes
     * a HEARTBEAT connection {@link ECSHeartbeatThread} between the ECS and the new server.
     *
     * @param address    Address of the new server to be added to the {@link ExternalConfigurationService}.
     * @param portString Port of the new server in String format.
     * @return A {@link KVMessage} indicating the status of the new server request.
     */
    private KVMessage start(String address, String portString) {
        try {
            int port = Integer.parseInt(portString);

            //start HEARTBEAT thread
            Socket connection = new Socket(address, port);
            new ECSHeartbeatThread(service, connection).run();
            service.addServer(address, port);

            //TODO Send back metadata? It will be sent anyway by the update message
            return new KVMessageImpl(StatusType.ECS_ACK);

        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to connect to {}:{}", address, portString);
            return new KVMessageImpl(StatusType.ERROR);
        } catch (NumberFormatException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Port number not valid while trying to connect to {}:{}", address, portString);
            return new KVMessageImpl(StatusType.ERROR);
        } catch (ECSException e) {
            LOGGER.fatal("Could not add new server");
            return new KVMessageImpl(StatusType.ERROR);
        }
    }

    /**
     * Method to initiate the graceful removal of a server from {@link ExternalConfigurationService}, and handoff
     * of the relevant key-value pairs to the responsible server.
     *
     * @param address    Address of the server that will shutdown.
     * @param portString Port of the server that will shut down, in String format.
     * @return A {@link KVMessage} with the status of the shutdown request.
     */
    private KVMessage serverShutdown(String address, String portString) {
        try {
            int port = Integer.parseInt(portString);
            service.removeServerAndHandoffData(address, port);
            return new KVMessageImpl(StatusType.ECS_ACK);

        } catch (NumberFormatException ex) {
            LOGGER.fatal("Port number not valid while trying to shut down" + address + ":" + portString);
            return new KVMessageImpl(StatusType.ERROR);
        } catch (ECSException e) {
            LOGGER.fatal("An error occurred during handoff while trying to shut down" + address + ":" + portString);
            return new KVMessageImpl(StatusType.ERROR);
        }
    }

}
