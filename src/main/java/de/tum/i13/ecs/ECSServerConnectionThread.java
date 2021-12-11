package de.tum.i13.ecs;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.CommunicationClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class ECSServerConnectionThread extends ECSThread{

    private static final Logger LOGGER = LogManager.getLogger(ECSServerConnectionThread.class);

    private CommandProcessor<String> cp;

    public ECSServerConnectionThread(CommandProcessor<String> commandProcessor, Socket server) throws IOException{
        super(server);
        this.cp = commandProcessor;
    }
    
    @Override
    public void run() {
        try {
            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = getActiveConnection().receive()) != null && !firstLine.equals("-1")) {
                String response = cp.process(firstLine);
                getActiveConnection().send(response);
            }

        } catch(CommunicationClientException ex) {
            LOGGER.atFatal().withThrowable(ex).log("Caught exception while trying to read from {}.", getSocket().getInetAddress());
        } catch(Exception ex){
            LOGGER.atFatal().withThrowable(ex).log("Caught exception while trying to close connection with {}.", getSocket().getInetAddress());
        }
    }
    
}
