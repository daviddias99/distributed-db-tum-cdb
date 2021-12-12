package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

class ECSUpdateMetadataThread extends ECSThread {

    private static final Logger LOGGER = LogManager.getLogger(ECSUpdateMetadataThread.class);

    private final KVMessage metadataMessage;

    ECSUpdateMetadataThread(NetworkLocation location, String metadata) throws IOException {
        super(location);
        this.metadataMessage = new KVMessageImpl(metadata, StatusType.ECS_SET_KEYRANGE);
    }

    @Override
    public void run() {
        LOGGER.info("Starting metadata update thread");
        try {
            LOGGER.trace(Constants.SENDING_AND_EXPECTING_MESSAGE, metadataMessage, StatusType.SERVER_ACK);
            sendAndReceiveMessage(metadataMessage, StatusType.SERVER_ACK);
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while connection to {} from ECS.", getSocket().getInetAddress());
        } catch (ECSException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught {} exception while communicating with server {}.", ex.getType(), getSocket());
        }

    }

}
