package de.tum.i13.ecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.hashing.HashingAlgorithm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to initiate HANDOFF of relevant key-value pairs between two servers.
 */
public class ECSHandoffThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ECSHandoffThread.class);

    private NetworkLocation successor;
    private NetworkLocation newServer;
    private BigInteger lowerBound;
    private BigInteger upperBound;

    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;
    
    public ECSHandoffThread(NetworkLocation successor, NetworkLocation newServer, BigInteger lowerBound, BigInteger upperBound){
        this.successor = successor;
        this.newServer = newServer;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public void run() {

        LOGGER.info("Trying to establish a connection to server {}", successor.getAddress());
        try (final Socket ecsSocket = new Socket(successor.getAddress(), successor.getPort())) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing ECS connection to {}.", successor.getAddress());
                try {
                    ecsSocket.close();
                } catch (IOException ex) {
                    LOGGER.fatal("Caught exception, while closing ECS socket", ex);
                }
            }));

            //TODO Change communication class?
            //set up the communication channel with the successor server
            setUpCommunication(ecsSocket);

            //send ECS_WRITE_LOCK message to successor and receive SERVER_WRITE_LOCK as response
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_LOCK), StatusType.SERVER_WRITE_LOCK);

            //Send ECS_HANDOFF message to the successor and wait to receive SERVER_HANDOFF_ACK
            sendAndReceiveMessage(prepareHandoffMessage(StatusType.ECS_HANDOFF), StatusType.SERVER_HANDOFF_ACK);

            //Wait for SERVER_HANDOFF_SUCCESS
            waitForHandoffSuccess();

            //Send ECS_WRITE_UNLOCK and receive SERVER_WRITE_UNLOCK
            sendAndReceiveMessage(new KVMessageImpl(StatusType.ECS_WRITE_UNLOCK), StatusType.SERVER_WRITE_UNLOCK);

            //ECS_SET_KEY_RANGE?

            activeConnection.close();

        } catch( IOException ex){
            LOGGER.fatal("Caught exception while connecting to {} from ECS.", successor.getAddress());
        } catch( ECSException ex){
            LOGGER.fatal("Caught " + ex.getType() + " exception while communicating with server {}. " + ex.getMessage(), successor.getAddress());
        } catch( Exception ex){
            LOGGER.fatal("Caught exception while closing connection to {}.", successor.getAddress());
        }
    }

    /**
     * Sets up the input and output streams to connect to the successor server.
     * @param ecsSocket {@link Socket} socket for the communication.
     * @throws IOException if communication cannot be established.
     */
    private void setUpCommunication(Socket ecsSocket) throws IOException{
        LOGGER.info("Trying to set up communication with server {}", successor.getAddress());
        in = new BufferedReader(new InputStreamReader(ecsSocket.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(new OutputStreamWriter(ecsSocket.getOutputStream(), Constants.TELNET_ENCODING));
        activeConnection = new ActiveConnection(ecsSocket, out, in);
    }

    /**
     * Sends a {@link KVMessage} message to the connected server and waits for a response. 
     * Checks the {@link StatusType} of the response against a provided expected type.
     * @param message a {@link KVMessage} object containing the message to be sent to server.
     * @param expectedType the {@link StatusType} object with the expecetd type of the response {@link KVMessage}.
     * @throws IOException if unable to read a response from the server.
     * @throws ECSException if communication is not as expected.
     */
    private void sendAndReceiveMessage(KVMessage message, KVMessage.StatusType expectedType) throws IOException, ECSException{
        activeConnection.write(message.packMessage());  //send a message

        String response = activeConnection.readline();  //wait for a response

        //check the response against expectation
        if(response == null || response == "-1"){
            String exceptionMessage = "Waiting for " + expectedType + ", no response received.";
            throw new ECSException( ECSException.Type.NO_ACK_RECEIVED, exceptionMessage);
        }
        else if(KVMessage.unpackMessage(response).getStatus() != expectedType){
            String exceptionMessage = "Waiting for " +  expectedType + ", received " + KVMessage.unpackMessage(response).getStatus() + ".";
            throw new ECSException( ECSException.Type.UNEXPECTED_RESPONSE, exceptionMessage);
        }

    }

    /**
     * Method to determine/distinguish if the initiated HANDOFF was successful or not.
     * @throws IOException if unable to read from the connection.
     */
    private void waitForHandoffSuccess() throws IOException{
        String response = activeConnection.readline();  //wait for SERVER_HANDOFF_SUCCESS

        if(response == null || response == "-1" || KVMessage.unpackMessage(response).getStatus() != StatusType.SERVER_HANDOFF_SUCCESS){
            //TODO What to do?
            LOGGER.fatal("Handoff failed or ack was not sent to ECS");
            //String exceptionMessage = "Handoff failed or ack was not sent to ECS";
            //throw new ECSException( ECSException.Type.HANDOFF_FAILURE, exceptionMessage);
        }
    }

    /**
     * Prepares and returns a ECS_HANDOFF message that contains the {@link NetworkLocation} of the new server 
     * and the range of keys to be sent to that server.
     * @param type {@link StatusType} type of the {@link KVMessage} to be returned.
     * @return a {@link KVMessage} to initiate handoff of key-value pairs from successor to the new server in the ring.
     */
    private KVMessage prepareHandoffMessage(StatusType type){
        String bound1 = HashingAlgorithm.convertHashToHexWithPrefix(this.lowerBound);
        String bound2 = HashingAlgorithm.convertHashToHexWithPrefix(this.upperBound);
        String peerNetworkLocation = NetworkLocation.packNetworkLocation(newServer);
        
        return new KVMessageImpl(peerNetworkLocation, bound1 + " " + bound2, type);
    }
   
}
