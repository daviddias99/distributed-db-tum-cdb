package de.tum.i13.simulator;

import static de.tum.i13.server.cache.CachingStrategy.LFU;
import static de.tum.i13.simulator.experiments.ExperimentConfiguration.experimentConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import de.tum.i13.simulator.experiments.Experiment;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import de.tum.i13.simulator.experiments.HardShutdownExperiment;
import de.tum.i13.simulator.experiments.ExperimentConfiguration.Builder;

public class Experiments {

    static void cacheExperiment() {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .afterAdditionalClientsDelay(120)
                .afterAdditionalServersDelay(15)
                .build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        scheduleExperiment(experiment);
    }

    static void smallExperiment() {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .startingServerCount(1)
                .startingClientCount(0)
                .initialDelay(10)
                .finalServerCount(5)
                .finalClientCount(5)
                .serverStartDelay(20)
                .clientStartDelay(20)
                .serverCacheSize(500)
                .bTreeNodeSize(100)
                .serverCachingStrategy(LFU)
                .statsName("small")
                .useChord()
                .replicationFactor(2)
                .afterAdditionalClientsDelay(60)
                .afterAdditionalServersDelay(60)
                .build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        scheduleExperiment(experiment);
    }

    static void reallySmall(boolean useChord) {
        final Builder experimentBuilder = experimentConfiguration()
                .startingServerCount(1)
                .startingClientCount(0)
                .initialDelay(10)
                .finalServerCount(2)
                .finalClientCount(2)
                .serverStartDelay(20)
                .clientStartDelay(20)
                .serverCacheSize(500)
                .bTreeNodeSize(100)
                .serverCachingStrategy(LFU)
                .statsName("really_small")
                .replicationFactor(2)
                .afterAdditionalClientsDelay(20)
                .afterAdditionalServersDelay(300);

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    static void replicationExperiment(boolean useChord, int replicaCount) {
        Builder experimentBuilder = experimentConfiguration()
                .initialDelay(10)
                .startingServerCount(5)
                .startingClientCount(0)
                .finalServerCount(5)
                .finalClientCount(10)
                .afterAdditionalServersDelay(120)
                .clientStartDelay(5)
                .serverCacheSize(500)
                .bTreeNodeSize(200)
                .serverCachingStrategy(LFU)
                .replicationFactor(replicaCount)
                .statsName(String.format("repl_%s_%d", useChord ? "chord" : "normal", replicaCount));

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    static void behaviorExperiment(boolean useChord) {
        Builder experimentBuilder = experimentConfiguration()
                .initialDelay(10)
                .startingServerCount(1)
                .startingClientCount(0)
                .finalServerCount(10)
                .finalClientCount(20)
                .afterAdditionalClientsDelay(120)
                .afterAdditionalServersDelay(120)
                .serverStartDelay(120)
                .clientStartDelay(20)
                .serverCacheSize(500)
                .bTreeNodeSize(200)
                .serverCachingStrategy(LFU)
                .replicationFactor(1)
                .statsName(String.format("behaviour_%s", useChord ? "chord" : "normal"));

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    private static void optionalChordHardShutdownExperiment(Builder experimentBuilder, boolean useChord) {
        if (useChord) {
            experimentBuilder = experimentBuilder.useChord();
        }

        final ExperimentConfiguration experimentConfiguration = experimentBuilder.build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        scheduleExperiment(experiment);
    }

    private static int scheduleExperiment(Experiment experiment) {
        final int offSetBeforeRun = experiment.scheduleBeforeRun();
        final int offSetRun = experiment.scheduleRun(offSetBeforeRun);
        return experiment.scheduleAfterRun(offSetRun);
    }
    public static void main(String[] args) {

        resetFolders();
        behaviorExperiment(true);
        // behaviorExperiment(false);
        // replicationExperiment(true, 0);
        // replicationExperiment(true, 2);
        // replicationExperiment(true, 5);
        // replicationExperiment(false, 0);
        // replicationExperiment(false, 2);
        // replicationExperiment(false, 5);
    }

    private static void resetFolders() {
        Experiments.deleteFolder(new File("logs"));
        Experiments.deleteFolder(new File("data"));
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    try {
                        Files.delete(f.toPath());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        try {
            Files.delete(folder.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
