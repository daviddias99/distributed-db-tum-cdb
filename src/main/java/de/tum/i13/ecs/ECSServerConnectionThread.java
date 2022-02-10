package de.tum.i13.ecs;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.CommunicationClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

class ECSServerConnectionThread extends ECSThread {

    private static final Logger LOGGER = LogManager.getLogger(ECSServerConnectionThread.class);

    private final CommandProcessor<String> cp;

    ECSServerConnectionThread(CommandProcessor<String> commandProcessor, Socket server) throws IOException {
        super(server);
        this.cp = commandProcessor;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server connection thread");
        try {
            //read messages from client and process using the CommandProcessor
            LOGGER.trace("Entering infinite loop to receive messages");
            String readLine;
            while ((readLine = getActiveConnection().receive()) != null && !readLine.equals("-1")) {
                LOGGER.trace("Delegating processing of read line '{}'", readLine);
                String response = cp.process(readLine);
                LOGGER.trace("Sending response after processing '{}'", response);
                getActiveConnection().send(response);
            }
        } catch (CommunicationClientException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to read from {}.", getSocket());
        } catch (Exception ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to close connection with {}.", getSocket());
        }
    }

}
