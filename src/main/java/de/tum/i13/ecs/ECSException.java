package de.tum.i13.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Preconditions;

/**
 * An Exception thrown when the communication between {@link ExternalConfigurationService} and server is not as expected
 */
public class ECSException extends Exception{

    private final Type type;
    private final String message;

    public ECSException(Type type, String message){
        Preconditions.notNull(type);
        Preconditions.notNull(message);

        this.type = type;
        this.message = message;
    }

    public ECSException(Type type){
        Preconditions.notNull(type);
        
        this.type = type;
        this.message = "No message provided";
    }

    /**
     * Getter method for the message parameter of {@link ECSException}.
     * @return String with the message related to the exception.
     */
    public String getMessage(){
        return this.message;
    }

    /**
     * Getter method for the {@link Type} parameter of {@link ECSException}.
     * @return {@link Type} object related to the exception.
     */
    public Type getType(){
        return this.type;
    }

    public static ECSException determineECSException(KVMessage.StatusType expectedType){
        return switch(expectedType){
            case SERVER_HANDOFF_SUCCESS -> new ECSException(Type.HANDOFF_FAILURE);
            case SERVER_ACK -> new ECSException(Type.NO_ACK_RECEIVED);
            default -> new ECSException(Type.UNEXPECTED_RESPONSE);
        };
    }


    public enum Type{
        /**
         * Indicates that the status of the response received by ECS is not the expected {@link KVMessage.StatusType} 
         */
        UNEXPECTED_RESPONSE,
        /**
         * Indicates that no acknowledgment was received by ECS when it was expected. 
         */
        NO_ACK_RECEIVED,
        /**
         * Indicates that Handoff of key-value servers failed or no acknowledgment was sent to ECS.
         */
        HANDOFF_FAILURE,
        /**
         * Indicates that the creation of a communication thread was not successful
         */
        THREAD_CREATION_FAILURE,
        /**
         * Indicates that the metadata of the server could not be updated by ECS. 
         */
        UPDATE_METADATA_FAILURE;
    }
    
}
