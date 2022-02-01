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
        for (serverNumber = 0; serverNumber < cfg.getFinalServerCount() - 1; serverNumber++) {
            (new Thread(new DelayedEvent(timeOffSetFromZero + serverNumber * cfg.getServerStartDelay(), DelayedEvent.Type.STOP_SERVER, mgr))).start();
        }
        return timeOffSetFromZero + serverNumber * cfg.getServerStartDelay();
    }

}
