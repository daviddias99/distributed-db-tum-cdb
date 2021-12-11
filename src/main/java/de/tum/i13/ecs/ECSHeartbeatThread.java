package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for establishing a connection with a server and continuosly sending a HEARTBEAT
 * message of {@link KVMessage} type. It waits ro receive a response within a constant amount of time,
 * otherwise considers the server to be down. The key value pairs are considered lost.
 */
class ECSHeartbeatThread extends ECSThread {

    private static final Logger LOGGER = LogManager.getLogger(ECSHeartbeatThread.class);

    ECSHeartbeatThread(Socket server) throws IOException {
        super(server);
    }

    @Override
    public void run() {
        LOGGER.info("Starting heartbeat thread");
        try {
            //set up the executor service to send the heartbeat message every second
            LOGGER.trace("Creating executor service");
            ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing heartbeat executor service");
                scheduledExecutor.shutdown();
            }));

            //set up waiting time for the response
            //TODO check if this works or needs to be set for every task
            LOGGER.trace("Setting heartbeat timeout and scheduling task");
            getSocket().setSoTimeout(Constants.HEARTBEAT_TIMEOUT_MILLISECONDS);
            scheduledExecutor.scheduleAtFixedRate(this::heartBeatTask, 0, Constants.SECONDS_PER_PING, TimeUnit.SECONDS);
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while establishing connection to {}.", getSocket());
        }
    }

    private void heartBeatTask() {
        try {
            LOGGER.trace("Sending heartbeat to {}", socket.getInetAddress());
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_HEART_BEAT), StatusType.SERVER_HEART_BEAT);
        } catch (SocketTimeoutException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Heartbeat timeout. Heartbeat from {} not detected after 700ms.", getSocket());
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while reading from {}.", getSocket());
        } catch (ECSException ex) {
            LOGGER.fatal("Caught ECSException of type {} when requesting heartbeat from {}", ex.getType(),
                    getSocket());
        }
    }

}
