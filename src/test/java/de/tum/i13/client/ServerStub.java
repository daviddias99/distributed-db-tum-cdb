package de.tum.i13.client;

import de.tum.i13.shared.Constants;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

class ServerStub implements Runnable {

    final ServerSocket localServer;

    ServerStub(ServerSocket serverSocket) {
        this.localServer = serverSocket;
    }

    synchronized Socket accept() {
        Socket clientSocket = null;
        try {
            clientSocket = this.localServer.accept();
        } catch (IOException e) {
            // e.printStackTrace();
        }
        return clientSocket;
    }

    @Override
    public void run() {

        byte[] serverGreet = "Welcome!\r\n".getBytes();
        byte[] serverAnswer = "Answer!\r\n".getBytes();

        byte[] buffer = new byte[Constants.MAX_MESSAGE_SIZE_BYTES];

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
