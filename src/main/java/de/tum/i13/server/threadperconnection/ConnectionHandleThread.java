package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.ActiveConnection;
import de.tum.i13.shared.net.CommunicationClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Runnable that handles a new connection to the server
 */
public class ConnectionHandleThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ConnectionHandleThread.class);

    private CommandProcessor<String> cp;
    private Socket clientSocket;
    private InetSocketAddress serverAddress;
    private ConnectionHandler connectionHandler;

    /**
     * Create new connection handler
     *
     * @param commandProcessor  command processor for incoming messages
     * @param connectionHandler handler for accepted and closing messages
     * @param clientSocket      socket of incoming communication
     * @param serverAddress     address of server socket
     */
    public ConnectionHandleThread(CommandProcessor<String> commandProcessor, ConnectionHandler connectionHandler,
                                  Socket clientSocket,
                                  InetSocketAddress serverAddress) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
        this.serverAddress = serverAddress;
        this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
        LOGGER.debug("Handling connection to {} in new thread", serverAddress);
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            ActiveConnection activeConnection = new ActiveConnection(clientSocket, out, in);
            // Send a confirmation message to peer upon connection if he needs the greet

            LOGGER.trace("({}) Sending greet to peer", Thread.currentThread().getName());

            String connSuccess = connectionHandler.connectionAccepted(this.serverAddress,
                    (InetSocketAddress) clientSocket.getRemoteSocketAddress());
            activeConnection.send(connSuccess);

            // read messages from client and process using the CommandProcessor
            String firstLine;
            while ((firstLine = activeConnection.receive()) != null && !firstLine.equals("-1")) {
                String response = cp.process(firstLine);

                if (!isHeartbeat(response)) {
                    LOGGER.info("({}) Peer message exchange in: {} out: {}", Thread.currentThread().getName(),
                            firstLine, response);
                }

                activeConnection.send(response);
            }

            LOGGER.trace("({}) Closing connection", Thread.currentThread().getName());
            activeConnection.close();
            connectionHandler.connectionClosed(clientSocket.getInetAddress());

        } catch (IOException ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to read from {}.", clientSocket.getInetAddress());
        } catch (CommunicationClientException ex) {
            LOGGER.fatal("Caught exception in communication component", ex);
        } catch (Exception ex) {
            LOGGER.atFatal()
                    .withThrowable(ex)
                    .log("Caught exception while trying to close connection with {}.", clientSocket.getInetAddress());
        }
    }

    private boolean isHeartbeat(String response) {
        return response.startsWith("server_heart_beat") || response.startsWith("chord_heartbeat");
    }

}
