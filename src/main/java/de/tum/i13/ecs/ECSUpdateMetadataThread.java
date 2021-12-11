package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.NetworkLocation;

import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class ECSUpdateMetadataThread extends ECSThread{

    private static final Logger LOGGER = LogManager.getLogger(ECSUpdateMetadataThread.class);

    private final KVMessage metadataMessage;

    public ECSUpdateMetadataThread(NetworkLocation location, String metadata) throws IOException{
        super(location);
        this.metadataMessage = new KVMessageImpl(metadata, StatusType.ECS_UPDATE_METADATA);
    }

    @Override
    public void run() {
        try {

            sendAndReceiveMessage(this.metadataMessage, StatusType.SERVER_ACK);

        } catch( IOException ex){
            LOGGER.fatal("Caught exception while connectin to {} from ECS.", getSocket().getInetAddress());
        } catch( ECSException ex){
            LOGGER.fatal("Caught " + ex.getType() + " exception while communicating with server {}. " + ex.getMessage(), getSocket().getInetAddress());
        }
        
    }
    
}
