package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Class to initiate HANDOFF of relevant key-value pairs between two servers.
 */
public class ECSHandoffThread extends ECSThread {

    private static final Logger LOGGER = LogManager.getLogger(ECSHandoffThread.class);

    private final KVMessage handoffMessage;
    
    public ECSHandoffThread(NetworkLocation successor, NetworkLocation newServer, BigInteger lowerBound, BigInteger upperBound) throws IOException{
        super(successor);
        this.handoffMessage = prepareHandoffMessage(newServer, lowerBound, upperBound);
    }

    @Override
    public void run() {

        try {
            //send ECS_WRITE_LOCK message to successor and receive SERVER_WRITE_LOCK as response
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_LOCK), StatusType.SERVER_WRITE_LOCK);

            //Send ECS_HANDOFF message to the successor and wait to receive SERVER_HANDOFF_ACK
            sendAndReceiveMessage(this.handoffMessage, StatusType.SERVER_HANDOFF_ACK);

            //Wait for SERVER_HANDOFF_SUCCESS
            waitForResponse(StatusType.SERVER_HANDOFF_SUCCESS);

            //Send ECS_WRITE_UNLOCK and receive SERVER_WRITE_UNLOCK
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_UNLOCK), StatusType.SERVER_WRITE_UNLOCK);

            //ECS_SET_KEY_RANGE?

        } catch( IOException ex){
            LOGGER.fatal("Caught exception while reading from {}.", getSocket().getInetAddress());
        } catch( ECSException ex){
            LOGGER.fatal("Caught " + ex.getType() + " exception while communicating with" + getSocket().getInetAddress());
        }
    }

    /**
     * Prepares and returns a ECS_HANDOFF message that contains the {@link NetworkLocation} of the new server 
     * and the range of keys to be sent to that server.
     * @return a {@link KVMessage} to initiate handoff of key-value pairs from successor to the new server in the ring.
     */
    private KVMessage prepareHandoffMessage(NetworkLocation newServer, BigInteger lowerBound, BigInteger upperBound){
        String bound1 = HashingAlgorithm.convertHashToHexWithPrefix(lowerBound);
        String bound2 = HashingAlgorithm.convertHashToHexWithPrefix(upperBound);
        String peerNetworkLocation = newServer.getAddress() + ":" + newServer.getPort();
        
        return new KVMessageImpl(peerNetworkLocation, bound1 + " " + bound2, StatusType.ECS_HANDOFF);
    }
   
}
