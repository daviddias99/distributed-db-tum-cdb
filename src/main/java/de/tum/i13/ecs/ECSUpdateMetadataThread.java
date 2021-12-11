package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class ECSUpdateMetadataThread extends ECSThread{

    private static final Logger LOGGER = LogManager.getLogger(ECSUpdateMetadataThread.class);

    private final KVMessage metadataMessage;

    public ECSUpdateMetadataThread(NetworkLocation location, String metadata) throws IOException{
        super(location);
        this.metadataMessage = new KVMessageImpl(metadata, StatusType.ECS_SET_KEYRANGE);
    }

    @Override
    public void run() {
        try {

            sendAndReceiveMessage(this.metadataMessage, StatusType.SERVER_ACK);

        } catch( IOException ex){
            LOGGER.fatal("Caught exception while connection to {} from ECS.", getSocket().getInetAddress());
        } catch( ECSException ex){
            LOGGER.fatal("Caught " + ex.getType() + " exception while communicating with server {}. " + ex.getMessage(), getSocket().getInetAddress());
        }
        
    }
    
}
