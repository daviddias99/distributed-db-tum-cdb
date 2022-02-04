package de.tum.i13.simulator.experiments;

class ExperimentException extends RuntimeException {

    ExperimentException(String message, Exception e) {
        super(message, e);
    }

}
