package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.PeerAuthenticator;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;
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

public class ConnectionHandleThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ConnectionHandleThread.class);

    private CommandProcessor<String> cp;
    private Socket clientSocket;
    private InetSocketAddress serverAddress;
    private ConnectionHandler connectionHandler;

    public ConnectionHandleThread(CommandProcessor<String> commandProcessor, ConnectionHandler connectionHandler, Socket clientSocket,
            InetSocketAddress serverAddress) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
        this.serverAddress = serverAddress;
        this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            ActiveConnection activeConnection = new ActiveConnection(clientSocket, out, in);
            PeerType peerType = (new PeerAuthenticator()).authenticate(activeConnection);

            // Send a confirmation message to peer upon connection if he needs the greet
            if (peerType.needsGreet()) {
                String connSuccess = connectionHandler.connectionAccepted(this.serverAddress,
                        (InetSocketAddress) clientSocket.getRemoteSocketAddress());
                activeConnection.write(connSuccess);
            }

            // read messages from client and process using the CommandProcessor
            String firstLine;
            while ((firstLine = activeConnection.readline()) != null && !firstLine.equals("-1")) {
                String response = cp.process(firstLine, peerType);
                activeConnection.write(response);
            }

            activeConnection.close();
            connectionHandler.connectionClosed(clientSocket.getInetAddress());

        } catch (IOException ex) {
            LOGGER.fatal("Caught exception while trying to read from {}.", clientSocket.getInetAddress());
        } catch (Exception ex) {
            LOGGER.fatal("Caught exception while trying to close connection with {}.", clientSocket.getInetAddress());
        }
    }
}
