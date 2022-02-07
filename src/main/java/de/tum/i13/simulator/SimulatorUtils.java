package de.tum.i13.simulator;

import de.tum.i13.shared.LogSetup;
import org.apache.logging.log4j.ThreadContext;

import java.nio.file.Path;

public class SimulatorUtils {

    private SimulatorUtils() {
    }

    public static Runnable wrapWarnLogging(Runnable runnable) {
        return () -> {
            ThreadContext.put("SIMULATOR", "true");
            runnable.run();
        };
    }

    public static Runnable withLoggedToDifferentFiles(Runnable runnable, Path logFile) {
        return () -> {
            LogSetup.setLogFile(logFile);
            runnable.run();
        };
    }

}
