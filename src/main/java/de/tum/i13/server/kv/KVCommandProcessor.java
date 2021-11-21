package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private PersistentStorage kvStore;
    private static final Logger LOGGER = LogManager.getLogger(KVCommandProcessor.class);

    public KVCommandProcessor(PersistentStorage storage) {
        this.kvStore = storage;
    }

    @Override
    public String process(String command) {
        
        try{
            String[] tokens = command.trim().split("\\s+");
            KVMessage message;

            switch (tokens[0]){
                case "get": message = this.get(tokens[1]); break;
                case "put": message = this.put(tokens[1], tokens[2]); break;
                case "delete": message = this.delete(tokens[1]);
                default: message = new KVMessageImpl(null, StatusType.UNDEFINED);
            }

           return message.toString();

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

    /**
     * Method to search for a key in the persistent storage. 
     * @param key the key to search
     * @return a KVMessage with the status of the query
     */
    private KVMessage get(String key){
        try{
            LOGGER.info("Trying to read key: {}", key);
            return kvStore.get(key);
        }catch(GetException e){
            LOGGER.error(e.getMessage());
            return new KVMessageImpl(key, StatusType.GET_ERROR);
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
            LOGGER.info("Trying to put key: {} and value: {}", key, value);
            return kvStore.put(key, value);
        }catch(Exception e){
            LOGGER.error(e.getMessage());
            return new KVMessageImpl(key, value, StatusType.PUT_ERROR);
        }
    }

    private KVMessage delete(String key){
        try{
            LOGGER.info("Trying to delte key: {}", key);
            return kvStore.put(key, null);
        }
        catch(Exception e){
            LOGGER.error(e.getMessage());
            return new KVMessageImpl(key, StatusType.DELETE_ERROR);
        }
    }

    /**
     * Method to "format" the reply to be sent to the user
     * @param message KVMessage received from the database
     * @return String message to send back to the client
     */
    //private String handleMessage(KVMessage message){
        
        //String status = message.getStatus().toString();
        //String response; 
        //switch(message.getStatus()){
        //    case GET_SUCCESS: response = ""; break;
        //    case GET_ERROR: response = ""; break;
        //   case PUT_SUCCESS: response = ""; break;
        //    case PUT_ERROR: response = ""; break;
        //    case PUT_UPDATE: response = ""; break;
        //    case DELETE_SUCCESS: response = ""; break;
        //    case DELETE_ERROR: response = ""; break;
        //   case UNDEFINED: response = ""; break;
        //    default: response = "";
        //}

        //return response;
    //}
}
