package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.ActiveConnection;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class ECSThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ECSThread.class);

    private Socket socket;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    public ECSThread(Socket socket) throws IOException {
        this.socket = socket;
        setUpCommunication(socket);
    }

    public ECSThread(NetworkLocation location) throws IOException {
        setUpCommunication(location);
    }

    @Override
    public void run() {
    }

    /**
     * Sets up the input and output streams to connect to a server at the goven
     * socket.
     * 
     * @param ecsSocket {@link Socket} socket for the communication.
     * @throws ECSException if communication cannot be established.
     */
    private void setUpCommunication(Socket ecsSocket) throws IOException {
        LOGGER.info("Trying to set up communication with server {}", ecsSocket.getInetAddress());
        in = new BufferedReader(new InputStreamReader(ecsSocket.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(new OutputStreamWriter(ecsSocket.getOutputStream(), Constants.TELNET_ENCODING));
        activeConnection = new ActiveConnection(ecsSocket, out, in);
    }

    /**
     * Creates a socket to communicate with the server at the given
     * {@link NetworkLocation}.
     * Sets up the input and output streams.
     * 
     * @param ecsSocket {@link Socket} socket for the communication.
     * @throws ECSException if communication cannot be established.
     */
    private void setUpCommunication(NetworkLocation serverLocation) throws IOException {
        LOGGER.info("Trying to set up communication with server {}", serverLocation.getAddress());

        Socket socket = new Socket(serverLocation.getAddress(), serverLocation.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Closing ECS connection to {}.", serverLocation.getAddress());
            try {
                activeConnection.close();
            } catch (Exception ex) {
                LOGGER.atFatal().withThrowable(ex).log("Caught exception, while closing ECS socket", ex);
            }
        }));

        this.socket = socket;
        in = new BufferedReader(new InputStreamReader(socket.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Constants.TELNET_ENCODING));
        activeConnection = new ActiveConnection(socket, out, in);
    }

    /**
     * Sends a {@link KVMessage} message to the connected server and waits for a
     * response.
     * Checks the {@link StatusType} of the response against a provided expected
     * type.
     * 
     * @param message      a {@link KVMessage} object containing the message to be
     *                     sent to server.
     * @param expectedType the {@link StatusType} object with the expecetd type of
     *                     the response {@link KVMessage}.
     * @throws IOException  if unable to read a response from the server.
     * @throws ECSException if communication is not as expected.
     */
    protected void sendAndReceiveMessage(KVMessage message, KVMessage.StatusType expectedType)
            throws IOException, ECSException {
        activeConnection.send(message.packMessage()); // send a message
        waitForResponse(expectedType);
    }

    /**
     * Method to wait for a response and determine if the response is as expected.
     * 
     * @param expectedType The {@link KVMessage.StatusType} type that the
     *                     {@link KVMessage} response must have.
     * @return the {@link KVMessage} response received from the server.
     * @throws IOException  if unable to read from the connection.
     * @throws ECSException if the expected response is different from what is
     *                      received.
     */
    protected KVMessage waitForResponse(KVMessage.StatusType expectedType) throws IOException, ECSException {
        String response = null;
        try {
            response = activeConnection.receive();
        } catch (CommunicationClientException exception) {
            throw new IOException("Could not receive", exception);
        }

        if (response == null || response == "-1") {
            throw new ECSException(ECSException.Type.NO_ACK_RECEIVED, "No response received from server");
        } else if (KVMessage.unpackMessage(response).getStatus() != expectedType) {
            throw ECSException.determineECSException(expectedType);
        }

        return KVMessage.unpackMessage(response);
    }

    /**
     * Getter for the socket param
     * 
     * @return the server connection socket
     */
    protected Socket getSocket() {
        return this.socket;
    }

    /**
     * Getter for {@link ActiveConnection} param
     * 
     * @return
     */
    protected ActiveConnection getActiveConnection() {
        return this.activeConnection;
    }
}
