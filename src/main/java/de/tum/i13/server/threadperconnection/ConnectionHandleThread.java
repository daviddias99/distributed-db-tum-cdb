package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.ActiveConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ConnectionHandleThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ConnectionHandleThread.class);
    
    private CommandProcessor cp;
    private Socket clientSocket;
    private InetSocketAddress serverAddress;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket, InetSocketAddress serverAddress) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
        this.serverAddress = serverAddress;
    }

    @Override
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            activeConnection = new ActiveConnection(clientSocket, out, in);

            //Send a confirmation message to client upon connection
            String connSuccess = cp.connectionAccepted(this.serverAddress, (InetSocketAddress) clientSocket.getRemoteSocketAddress());
            activeConnection.send(connSuccess);

            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = activeConnection.receive()) != null && !firstLine.equals("-1")) {
                String response = cp.process(firstLine);
                activeConnection.send(response);
            }

            activeConnection.close();
            cp.connectionClosed(clientSocket.getInetAddress());

        } catch(IOException ex) {
            LOGGER.fatal("Caught exception while trying to read from {}.", clientSocket.getInetAddress());
        } catch(Exception ex){
            LOGGER.fatal("Caught exception while trying to close connection with {}.", clientSocket.getInetAddress());
        }
    }
}
