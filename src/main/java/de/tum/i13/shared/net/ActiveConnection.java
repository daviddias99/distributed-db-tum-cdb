package de.tum.i13.shared.net;

import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by chris on 19.10.15.
 */
public class ActiveConnection implements AutoCloseable, MessageServer {

    private final Socket socket;
    private final PrintWriter output;
    private final BufferedReader input;

    public ActiveConnection(Socket socket, PrintWriter output, BufferedReader input) {
        this.socket = socket;

        this.output = output;
        this.input = input;
    }

    @Override
    public void send(String command) {
        output.write(command + Constants.TERMINATING_STR);
        output.flush();
    }

    @Override
    public String receive() throws CommunicationClientException {
        try {
            return input.readLine();
        } catch (IOException exception) {
            throw new CommunicationClientException(
                    exception,
                    CommunicationClientException.Type.INTERNAL_ERROR,
                    "Could not receive");
        }
    }

    /**
     * @throws IOException if this connection cannot be closed
     */
    @Override
    public void close() throws IOException {
        output.close();
        input.close();
        socket.close();
    }

    // TODO: check this
    public NetworkLocation getNetworkLocation() {
        return new NetworkLocationImpl(this.socket.getInetAddress().getHostAddress(), this.socket.getPort());
    }
}
