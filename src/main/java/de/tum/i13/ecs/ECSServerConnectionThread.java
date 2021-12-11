package de.tum.i13.ecs;

import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class ECSServerConnectionThread implements Runnable{

    private static final Logger LOGGER = LogManager.getLogger(ECSServerConnectionThread.class);

    private final Socket connectedServer;
    private CommandProcessor cp;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    public ECSServerConnectionThread(CommandProcessor commandProcessor, Socket server){
        this.connectedServer = server;
        this.cp = commandProcessor;
    }
    
    @Override
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(connectedServer.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(new OutputStreamWriter(connectedServer.getOutputStream(), Constants.TELNET_ENCODING));

            activeConnection = new ActiveConnection(connectedServer, out, in);

            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = activeConnection.readline()) != null && firstLine != "-1") {
                String response = cp.process(firstLine);
                activeConnection.write(response);
            }

            activeConnection.close();

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while trying to read from {}.", connectedServer.getInetAddress());
        } catch(Exception ex){
            LOGGER.fatal("Caught exception while trying to close connection with {}.", connectedServer.getInetAddress());
        }
    }
    
}
