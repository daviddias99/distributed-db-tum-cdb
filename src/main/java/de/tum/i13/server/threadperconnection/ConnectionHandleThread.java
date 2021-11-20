package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.ActiveConnection;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ConnectionHandleThread implements Runnable {
    
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
            activeConnection.write(connSuccess);

            //read messages from client and process using the CommandProcessor 
            String firstLine;
            while ((firstLine = activeConnection.readline()) != null && firstLine != "-1") {
                String response = cp.process(firstLine);
                activeConnection.write(response);
            }

            activeConnection.close();
            cp.connectionClosed(clientSocket.getInetAddress());

        //Logging: connection closed + IP address of client
        } catch(Exception ex) {
            cp.connectionInterrupted(clientSocket.getInetAddress());
        }
    }
}
