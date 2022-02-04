package de.tum.i13.simulator.experiments;

import static de.tum.i13.simulator.events.DelayedEvent.Type.STOP_SERVER;

import de.tum.i13.simulator.events.DelayedEvent;

public class SoftShutdownExperiment extends AbstractExperiment {

    public SoftShutdownExperiment(ExperimentConfiguration experimentConfiguration) {
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
            new DelayedEvent(timeOffSetFromZero + serverNumber * cfg.getServerStartDelay(), STOP_SERVER, mgr)
                    .schedule();
        }
        return timeOffSetFromZero + serverNumber * cfg.getServerStartDelay();
    }

}
