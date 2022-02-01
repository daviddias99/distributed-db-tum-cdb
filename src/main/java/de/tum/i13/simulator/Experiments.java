package de.tum.i13.simulator;

import static de.tum.i13.server.cache.CachingStrategy.LFU;
import static de.tum.i13.simulator.ExperimentConfiguration.experimentConfiguration;

class Experiments {

    static void cacheExperiment() {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .afterAdditionalClientsDelay(120)
                .aAfterAdditionalServersDelay(15)
                .build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        scheduleExperiment(experiment);
    }

    static void behaviorExperiment() {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .startingServerCount(1)
                .startingClientCount(0)
                .finalServerCount(10)
                .finalClientCount(20)
                .serverStartDelay(60)
                .clientStartDelay(20)
                .serverCacheSize(500)
                .bTreeNodeSize(100)
                .serverCachingStrategy(LFU)
                .statsName("behavior")
                .afterAdditionalClientsDelay(120)
                .aAfterAdditionalServersDelay(120)
                .build();
        final Experiment experiment = new SoftShutdownExperiment(experimentConfiguration);
        scheduleExperiment(experiment);
    }

    private static int scheduleExperiment(Experiment experiment) {
        final int offSetBeforeRun = experiment.scheduleBeforeRun();
        final int offSetRun = experiment.scheduleRun(offSetBeforeRun);
        return experiment.scheduleAfterRun(offSetRun);
    }

    public static void main(String[] args) {
        cacheExperiment();
    }

}
