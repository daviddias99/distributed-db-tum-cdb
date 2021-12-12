package de.tum.i13.shared.net;

import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by chris on 19.10.15.
 */
public class ActiveConnection implements AutoCloseable, MessageServer {

    private static final Logger LOGGER = LogManager.getLogger(ActiveConnection.class);

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
        LOGGER.trace("Sending message to {}: '{}'", socket, command);
        output.write(command + Constants.TERMINATING_STR);
        output.flush();
    }

    @Override
    public String receive() throws CommunicationClientException {
        LOGGER.trace("Trying to receive message");
        try {
            final String readLine = input.readLine();
            LOGGER.trace("Received message from {}: '{}'", socket, readLine);
            return readLine;
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
        LOGGER.trace("Closing connection to {}", socket);
        output.close();
        input.close();
        socket.close();
    }

    // TODO: check this
    public NetworkLocation getNetworkLocation() {
        return new NetworkLocationImpl(this.socket.getInetAddress().getHostAddress(), this.socket.getPort());
    }

    @Override
    public String toString() {
        return "ActiveConnection{" +
                "socket=" + socket +
                '}';
    }

}
