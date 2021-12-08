package de.tum.i13.shared;

/**
 * Represents a class that can process commands. A command leads to an answer of
 * the same type (or null if no answer is due).
 */
public interface CommandProcessor<T> {

    /**
     * Process {@code command}. Returns the answer to the command, or null if no answer.
     * @param command Command to process
     * @return Answer to command or null if no answer.
     */
    T process(T command);
}
