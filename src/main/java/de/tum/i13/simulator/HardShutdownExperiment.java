package de.tum.i13.simulator;

class HardShutdownExperiment extends AbstractExperiment {

    HardShutdownExperiment(ExperimentConfiguration experimentConfiguration) {
        super(experimentConfiguration);
    }


    @Override
    public int scheduleBeforeRun() {
        return 0;
    }

    @Override
    public int scheduleAfterRun(int timeOffSetFromZero) {
        (new Thread(new DelayedEvent(timeOffSetFromZero, DelayedEvent.Type.STOP_PROGRAM, mgr))).start();
        return timeOffSetFromZero;
    }

}
