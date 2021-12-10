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
import java.net.InetSocketAddress;
import java.net.Socket;

public class ECSServerThread implements Runnable{

    private static final Logger LOGGER = LogManager.getLogger(ECSServerThread.class);

    private final Socket connectedServer;
    private CommandProcessor cp;
    private InetSocketAddress serverAddress;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    public ECSServerThread(CommandProcessor commandProcessor, Socket server){
        this.connectedServer = server;
        this.cp = commandProcessor;
    }
    
    @Override
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(connectedServer.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(new OutputStreamWriter(connectedServer.getOutputStream(), Constants.TELNET_ENCODING));

            activeConnection = new ActiveConnection(connectedServer, out, in);

            //Send a confirmation message to client upon connection
            String connSuccess = cp.connectionAccepted(this.serverAddress, (InetSocketAddress) connectedServer.getRemoteSocketAddress());
            activeConnection.write(connSuccess);

            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = activeConnection.readline()) != null && firstLine != "-1") {
                String response = cp.process(firstLine);
                activeConnection.write(response);
            }

            activeConnection.close();
            cp.connectionClosed(connectedServer.getInetAddress());

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while trying to read from {}.", connectedServer.getInetAddress());
        } catch(Exception ex){
            LOGGER.fatal("Caught exception while trying to close connection with {}.", connectedServer.getInetAddress());
        }
    }
    
}
