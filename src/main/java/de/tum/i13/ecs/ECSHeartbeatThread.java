package de.tum.i13.ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.Constants;

/**
 * Class responsible for establishing a connection with a server and continuosly sending a HEARTBEAT
 * message of {@link KVMessage} type. It waits ro receive a response within a constant amount of time, 
 * otherwise considers the server to be down. The key value pairs are considered lost.
 */
public class ECSHeartbeatThread implements Runnable{

    private static final Logger LOGGER = LogManager.getLogger(ECSHeartbeatThread.class);

    //heartbeat message to be sent
    private final String heartbeatMessage = new KVMessageImpl(StatusType.ECS_HEART_BEAT).packMessage();

    private final Socket connectedServer;
    private final ExternalConfigurationService service;
    private ScheduledExecutorService scheduledExecutor;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    public ECSHeartbeatThread(ExternalConfigurationService service, Socket server){
        this.service = service;
        this.connectedServer = server;
    }

    @Override
    public void run() {
        try {
            //set up the communication channel with the server
            in = new BufferedReader(new InputStreamReader(connectedServer.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(new OutputStreamWriter(connectedServer.getOutputStream(), Constants.TELNET_ENCODING));
            activeConnection = new ActiveConnection(connectedServer, out, in);

            //set up the executor service to send the heartbeat message every second
            scheduledExecutor = Executors.newScheduledThreadPool(0);

            Runnable heartbeatTask = () -> {
                try{
                    //send heartbeat message
                    activeConnection.write(heartbeatMessage);

                    //receive and store response                 
                    String response = activeConnection.readline(); 

                    KVMessage responseMessage = KVMessage.unpackMessage(response);
                    if(responseMessage.getStatus() != StatusType.SERVER_HEART_BEAT){
                        LOGGER.fatal("Invalid or no response message received from {} after requesting heartbeat.", connectedServer.getInetAddress());
                        scheduledExecutor.shutdownNow();
                    }

                    //close active connection if no heartbeat received
                    if(scheduledExecutor.isShutdown())
                        activeConnection.close();

                } catch( IllegalArgumentException ex){
                    LOGGER.fatal("Invalid response message from {} after requesting heartbeat.", connectedServer.getInetAddress());
                } catch( SocketTimeoutException ex){
                    LOGGER.fatal("Heartbeat from {} not detected after 700ms.", connectedServer.getInetAddress());
                } catch( IOException ex){
                    LOGGER.fatal("Caught exception while reading from {}.", connectedServer.getInetAddress());
                } catch( Exception ex){
                    LOGGER.fatal("Caught exception while trying to close connection with {}.", connectedServer.getInetAddress());
                }
            };

            //set up waiting time for the response
            //TODO check if this works or needs to be set for every task 
            connectedServer.setSoTimeout(Constants.HEARTBEAT_TIMEOUT_MILLISECONDS); 
            scheduledExecutor.scheduleAtFixedRate(heartbeatTask, 0, Constants.SECONDS_PER_PING, TimeUnit.SECONDS);

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while establishing connection to {}.", connectedServer.getInetAddress());
        } finally{
            //Remove server from the ExternalConfigurationService and update metadata
            service.removeServer(connectedServer.getInetAddress().getHostAddress(), connectedServer.getPort());
        }
    }
}
