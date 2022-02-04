package de.tum.i13.simulator.experiments;

public interface Experiment {

    default int scheduleExperiment() {
        final int offSetBeforeRun = scheduleBeforeRun();
        final int offSetRun = scheduleRun(offSetBeforeRun);
        return scheduleAfterRun(offSetRun);
    }

    int scheduleBeforeRun();

    int scheduleRun(int timeOffsetFromZero);

    int scheduleAfterRun(int timeOffSetFromZero);

}
