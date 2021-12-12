package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.SocketException;
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
    private final ScheduledExecutorService scheduledExecutor;

    ECSHeartbeatThread(NetworkLocation networkLocation) throws IOException {
        super(networkLocation);
        //set up the executor service to send the heartbeat message every second
        LOGGER.trace("Creating executor service");
        scheduledExecutor = Executors.newScheduledThreadPool(1);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutDownExecutor));
    }

    @Override
    public void run() {
        LOGGER.info("Starting heartbeat thread");
        //set up waiting time for the response
        //TODO check if this works or needs to be set for every task
        LOGGER.trace("Setting heartbeat timeout and scheduling task");
        try {
            getSocket().setSoTimeout(Constants.HEARTBEAT_TIMEOUT_MILLISECONDS);
        } catch (SocketException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while establishing connection to {}.", getSocket());
        }
        scheduledExecutor.scheduleAtFixedRate(this::heartBeatTask, 0, Constants.SECONDS_PER_PING, TimeUnit.SECONDS);
    }

    private void heartBeatTask() {
        try {
            LOGGER.trace("Sending heartbeat to {}", getSocket());
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_HEART_BEAT), StatusType.SERVER_HEART_BEAT);
        } catch (SocketTimeoutException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Heartbeat timeout. Heartbeat from {} not detected after 700ms.", getSocket());
            shutDownExecutor();
        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while reading from {}.", getSocket());
        } catch (ECSException ex) {
            LOGGER.fatal("Caught ECSException of type {} when requesting heartbeat from {}", ex.getType(),
                    getSocket());
        }
    }

    private void shutDownExecutor() {
        LOGGER.info("Shutting down heartbeat executor service");
        scheduledExecutor.shutdown();

        //TODO Check if the address given here is correct
        ExternalConfigurationService.removeServer(getSocket().getInetAddress().toString(), getSocket().getPort());
    }

}
