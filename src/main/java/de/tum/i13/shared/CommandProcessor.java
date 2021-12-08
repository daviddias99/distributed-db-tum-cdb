package de.tum.i13.shared;

public interface CommandProcessor<T> {

    T process(T command);
}
