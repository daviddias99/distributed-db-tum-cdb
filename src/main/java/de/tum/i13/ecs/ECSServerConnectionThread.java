package de.tum.i13.ecs;

import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class ECSServerConnectionThread extends ECSThread{

    private static final Logger LOGGER = LogManager.getLogger(ECSServerConnectionThread.class);

    private CommandProcessor cp;

    public ECSServerConnectionThread(CommandProcessor commandProcessor, Socket server) throws IOException{
        super(server);
        this.cp = commandProcessor;
    }
    
    @Override
    public void run() {
        try {
            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = getActiveConnection().readline()) != null && firstLine != "-1") {
                String response = cp.process(firstLine);
                getActiveConnection().write(response);
            }

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while trying to read from {}.", getSocket().getInetAddress());
        } catch(Exception ex){
            LOGGER.fatal("Caught exception while trying to close connection with {}.", getSocket().getInetAddress());
        }
    }
    
}
