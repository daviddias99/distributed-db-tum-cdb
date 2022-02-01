package de.tum.i13.simulator;

class SoftShutdownExperiment extends AbstractExperiment {

    SoftShutdownExperiment(ExperimentConfiguration experimentConfiguration) {
        super(experimentConfiguration);
    }

    @Override
    public int scheduleBeforeRun() {
        return 0;
    }

    @Override
    public int scheduleAfterRun(int timeOffSetFromZero) {
        int serverNumber;
        for (serverNumber = 0; serverNumber < experimentConfiguration.getFinalServerCount() - 1; serverNumber++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay(), DelayedEvent.Type.STOP_SERVER, experimentManager))).start();
        }
        return timeOffSetFromZero + serverNumber * experimentConfiguration.getServerStartDelay();
    }

}
