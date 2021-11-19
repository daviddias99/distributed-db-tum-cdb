package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

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

    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket, InetSocketAddress serverAddress) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
        this.serverAddress = serverAddress;
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            //
            String connSuccess = cp.connectionAccepted(this.serverAddress, (InetSocketAddress) clientSocket.getRemoteSocketAddress());
            out.write(connSuccess);
            out.flush();

            String firstLine;
            while ((firstLine = in.readLine()) != null && firstLine != "-1") {
                String res = cp.process(firstLine);
                out.write(res + "\r\n");
                out.flush();
            }

            cp.connectionClosed(clientSocket.getInetAddress());

        //Logging: connection closed + IP address of client
        } catch(Exception ex) {
            cp.connectionClosed(clientSocket.getInetAddress());
        }
    }
}
