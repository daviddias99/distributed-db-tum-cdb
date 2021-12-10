package de.tum.i13.ecs;

/**
 * An Exception thrown when the communication between {@link ExternalConfigurationService} and server is not as expected
 */
public class ECSException extends Exception{

    private final Type type;
    private final String message;

    public ECSException(Type type, String message){
        this.type = type;
        this.message = message;
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
        HANDOFF_FAILURE;
    }
    
}
