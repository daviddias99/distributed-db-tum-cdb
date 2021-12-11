package de.tum.i13.ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.Constants;

/**
 * Class responsible for establishing a connection with a server and continuosly sending a HEARTBEAT
 * message of {@link KVMessage} type. It waits ro receive a response within a constant amount of time, 
 * otherwise considers the server to be down. The key value pairs are considered lost.
 */
public class ECSHeartbeatThread extends ECSThread{

    private static final Logger LOGGER = LogManager.getLogger(ECSHeartbeatThread.class);

    private final ExternalConfigurationService service;
    private ScheduledExecutorService scheduledExecutor;

    public ECSHeartbeatThread(ExternalConfigurationService service, Socket server) throws IOException{
        super(server);
        this.service = service;
    }

    @Override
    public void run() {
        try {
            //set up the executor service to send the heartbeat message every second
            scheduledExecutor = Executors.newScheduledThreadPool(0);

            Runnable heartbeatTask = () -> {
                try{
                   sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_HEART_BEAT), StatusType.SERVER_HEART_BEAT);

                } catch( SocketTimeoutException ex){
                    LOGGER.fatal("Heartbeat timeout. Heartbeat from {} not detected after 700ms.", getSocket().getInetAddress());
                } catch( IOException ex){
                    LOGGER.fatal("Caught exception while reading from {}.", getSocket().getInetAddress());
                } catch( ECSException ex){
                    LOGGER.fatal("Caught ECSException of type " + ex.getType() + " when requesting heartbeat from " + getSocket().getInetAddress());
                }
            };

            //set up waiting time for the response
            //TODO check if this works or needs to be set for every task 
            getSocket().setSoTimeout(Constants.HEARTBEAT_TIMEOUT_MILLISECONDS); 
            scheduledExecutor.scheduleAtFixedRate(heartbeatTask, 0, Constants.SECONDS_PER_PING, TimeUnit.SECONDS);

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while establishing connection to {}.", getSocket().getInetAddress());
        } finally{
            //Remove server from the ExternalConfigurationService and update metadata
            service.removeServer(getSocket().getInetAddress().getHostAddress(), getSocket().getPort());
            scheduledExecutor.shutdownNow();
        }
//        finally{
//            //Remove server from the ExternalConfigurationService and update metadata
//            service.removeServer(getSocket().getInetAddress().getHostAddress(), getSocket().getPort());
//            scheduledExecutor.shutdownNow();
//        }
    }
}
