package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private KVStore kvStore;
    private static final Logger LOGGER = LogManager.getLogger(KVCommandProcessor.class);

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public String process(String command) {
        
        try{
            String[] tokens = command.trim().split("\\s+");
            KVMessage message;

            switch (tokens[0]){
                case "get": message = this.get(tokens[1]); break;
                case "put": message = this.put(tokens[1], tokens[2]); break;
                //case "delete": message = this.delete(tokens[1]);
                default: message = new KVMessageForStub(StatusType.UNDEFINED);
            }

            return handleMessage(message);

        } catch(Exception e){
            return "Error!";
        }

    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: {}", remoteAddress);

        return "Connection to KVServer established: " + address.toString();
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        LOGGER.info("connection closed: {}", remoteAddress);
    }

    @Override
    public void connectionInterrupted(InetAddress remoteAddress){
        LOGGER.info("connection interrupted: {}", remoteAddress);
    }

    /**
     * Helper method to query the database
     * @param key the key to search
     * @return a KVMessage with the status of the query
     */
    private KVMessage get(String key){
        try{
            return kvStore.get(key);
        //TODO Change Exception Type
        }catch(Exception e){
            return new KVMessageForStub(StatusType.GET_ERROR, key, null);
        }
    }

    /**
     * Helper method to store a new KV pair in the database
     * @param key the key of the new pair
     * @param value the new value
     * @return a KVMessage with the status of the put operation
     */
    private KVMessage put(String key, String value){
        try{
            return kvStore.put(key, value);
        }catch(Exception e){
            return new KVMessageForStub(StatusType.PUT_ERROR, key, value);
        }
    }

    private KVMessage delete(String key){
        try{
            return kvStore.put(key, null);
        }
        catch(Exception e){
            return new KVMessageForStub(StatusType.DELETE_ERROR);
        }
    }

    /**
     * Method to "format" the reply to be sent to the user
     * @param message KVMessage received from the database
     * @return String message to send back to the client
     */
    private String handleMessage(KVMessage message){
        return message.getStatus().toString();
    }
}
