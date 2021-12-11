package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.ActiveConnection;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.NetworkLocation;

import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class ECSUpdateMetadataThread implements Runnable{

    private static final Logger LOGGER = LogManager.getLogger(ECSUpdateMetadataThread.class);

    private final NetworkLocation serverLocation;
    private ActiveConnection activeConnection;
    private BufferedReader in;
    private PrintWriter out;

    private final String metadata;

    public ECSUpdateMetadataThread(NetworkLocation location, String metadata){
        this.serverLocation = location;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        try (final  Socket socket = new Socket(serverLocation.getAddress(), serverLocation.getPort())) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing ECS connection to {}.", serverLocation.getAddress());
                try {
                    socket.close();
                } catch (IOException ex) {
                    LOGGER.fatal("Caught exception, while closing ECS socket", ex);
                }
            }));

            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Constants.TELNET_ENCODING));
            activeConnection = new ActiveConnection(socket, out, in);

            sendAndReceiveMessage(prepareMetadataMessage(this.metadata), StatusType.SERVER_ACK);

            activeConnection.close();

        } catch( IOException ex){
            LOGGER.fatal("Caught exception while connectin to {} from ECS.", serverLocation.getAddress());
        } catch( ECSException ex){
            LOGGER.fatal("Caught " + ex.getType() + " exception while communicating with server {}. " + ex.getMessage(), serverLocation.getAddress());
        } catch( Exception ex){
            LOGGER.fatal("Caught exception while closing connection to {}.", serverLocation.getAddress());
        }
        
    }

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

    private KVMessage prepareMetadataMessage( String metadata){
        return new KVMessageImpl(metadata, StatusType.ECS_UPDATE_METADATA);
    }
    
}
