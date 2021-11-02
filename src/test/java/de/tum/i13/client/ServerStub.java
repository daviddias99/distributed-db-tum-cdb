package de.tum.i13.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class ServerStub implements Runnable {

    private ServerSocket localServer;
    public int connectionsMade = 0;

    public static final int BYTES_PER_KB = 1024;
    public static final int MAX_MESSAGE_SIZE_KB = 128;
    public static final int MAX_MESSAGE_SIZE_BYTES = MAX_MESSAGE_SIZE_KB * BYTES_PER_KB;

    public ServerStub(ServerSocket serverSocket) {
        this.localServer = serverSocket;
    }

    public synchronized Socket accept() {
        Socket clientSocket = null;
        try {
            connectionsMade++;
            clientSocket = this.localServer.accept();
        } catch (IOException e) {
            // e.printStackTrace();
        }
        return clientSocket;
    }

    @Override
    public void run() {

        byte[] serverGreet = "Welcome!".getBytes();
        byte[] serverAnswer = "Answer!".getBytes();

        byte[] buffer = new byte[MAX_MESSAGE_SIZE_BYTES];

        while (true) {
            try {
                Socket clientSocket = this.accept();

                if(clientSocket == null) {
                    continue;
                }

                clientSocket.getOutputStream().write(serverGreet);

                while (true) {
                    int readResult = clientSocket.getInputStream().read(buffer);
                    if (readResult == -1) {
                        break;
                    }
                    clientSocket.getOutputStream().write(serverAnswer);
                }
            } catch (UnknownHostException e) {
                // e.printStackTrace();
            } catch (IOException e) {
                // e.printStackTrace();
            }
        }
    }
}
