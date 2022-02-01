package de.tum.i13.simulator;

interface Experiment {

    int scheduleBeforeRun();

    int scheduleRun(int timeOffsetFromZero);

    int scheduleAfterRun(int timeOffSetFromZero);

}
