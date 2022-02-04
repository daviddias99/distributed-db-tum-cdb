package de.tum.i13.simulator.experiments;

import de.tum.i13.simulator.events.DelayedEvent;
import de.tum.i13.simulator.events.DelayedEvent.Type;

public class HardShutdownExperiment extends AbstractExperiment {

    public HardShutdownExperiment(ExperimentConfiguration experimentConfiguration) {
        super(experimentConfiguration);
    }


    @Override
    public int scheduleBeforeRun() {
        return 0;
    }

    @Override
    public int scheduleAfterRun(int timeOffSetFromZero) {
        new DelayedEvent(timeOffSetFromZero, DelayedEvent.Type.STOP_PROGRAM, mgr).schedule();
        return timeOffSetFromZero;
    }

}
