package de.tum.i13.simulator;

import de.tum.i13.simulator.experiments.Experiment;
import de.tum.i13.simulator.experiments.ExperimentConfiguration;
import de.tum.i13.simulator.experiments.ExperimentConfiguration.Builder;
import de.tum.i13.simulator.experiments.HardShutdownExperiment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Stream;

import static de.tum.i13.server.cache.CachingStrategy.LFU;
import static de.tum.i13.simulator.experiments.ExperimentConfiguration.experimentConfiguration;

public class Experiments {

    private static final Logger LOGGER = LogManager.getLogger(Experiments.class);

    static void cacheExperiment() {
        final ExperimentConfiguration experimentConfiguration = experimentConfiguration()
                .afterAdditionalClientsDelay(120)
                .afterAdditionalServersDelay(15)
                .build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        experiment.scheduleExperiment();
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
        experiment.scheduleExperiment();
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
                .startingServerCount(6)
                .startingClientCount(20)
                .finalServerCount(6)
                .finalClientCount(0)
                .afterAdditionalServersDelay(180)
                .clientStartDelay(0)
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
                .startingServerCount(3)
                .startingClientCount(0)
                .finalServerCount(6)
                .finalClientCount(20)
                .afterAdditionalClientsDelay(120)
                .afterAdditionalServersDelay(120)
                .serverStartDelay(120)
                .clientStartDelay(5)
                .serverCacheSize(500)
                .bTreeNodeSize(200)
                .serverCachingStrategy(LFU)
                .replicationFactor(1)
                .statsName(String.format("behaviour_%s", useChord ? "chord" : "normal"));

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    static void replicationExperiment(boolean useChord) {
        // WIP
        Builder experimentBuilder = experimentConfiguration()
                .initialDelay(10)
                .startingServerCount(1)
                .startingClientCount(0)
                .finalServerCount(3)
                .finalClientCount(15)
                .afterAdditionalClientsDelay(10)
                .afterAdditionalServersDelay(240)
                .serverStartDelay(90)
                .clientStartDelay(5)
                .serverCacheSize(500)
                .bTreeNodeSize(200)
                .serverCachingStrategy(LFU)
                .replicationFactor(2)
                .statsName(String.format("behaviour_%s", useChord ? "chord" : "normal"));

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    static void noReplicationExperiment(boolean useChord) {
        // WIP
        Builder experimentBuilder = experimentConfiguration()
                .initialDelay(10)
                .startingServerCount(25)
                .startingClientCount(0)
                .finalServerCount(30)
                .finalClientCount(20)
                .afterAdditionalClientsDelay(10)
                .afterAdditionalServersDelay(120)
                .serverStartDelay(30)
                .clientStartDelay(5)
                .serverCacheSize(500)
                .bTreeNodeSize(200)
                .serverCachingStrategy(LFU)
                .replicationFactor(0)
                .statsName(String.format("behaviour_%s", useChord ? "chord" : "normal"));

        Experiments.optionalChordHardShutdownExperiment(experimentBuilder, useChord);
    }

    private static void optionalChordHardShutdownExperiment(Builder experimentBuilder, boolean useChord) {
        final var experimentConfiguration = experimentBuilder.useChord(useChord)
                .build();
        final Experiment experiment = new HardShutdownExperiment(experimentConfiguration);
        experiment.scheduleExperiment();
    }

    public static void main(String[] args) {
        resetFolders();
        // noReplicationExperiment(false);
        // replicationExperiment(true);
        // behaviorExperiment(true);
        // replicationExperimentPok(true, 0);
        // replicationExperiment(true, 1);
        // replicationExperiment(true, 2);
        // replicationExperiment(false, 0);
        // replicationExperiment(false, 1);
        replicationExperiment(false, 2);
    }

    private static void resetFolders() {
        Experiments.deleteFolder(new File("data"));
        Experiments.deleteFolder(new File("logs"));
    }

    private static void deleteFolder(File files) {
        if (!files.exists()) return;
        if (files.isDirectory()) {
            Stream.ofNullable(files.listFiles()) //some JVMs return null for empty dirs
                    .flatMap(Arrays::stream)
                    .forEach(Experiments::deleteFolder);
        }
        try {
            LOGGER.debug("Trying to delete file {}", files);
            Files.delete(files.toPath());
        } catch (IOException e) {
            LOGGER.atWarn()
                    .withThrowable(e)
                    .log("Couldn't delete file {}", files);
        }
    }

}
