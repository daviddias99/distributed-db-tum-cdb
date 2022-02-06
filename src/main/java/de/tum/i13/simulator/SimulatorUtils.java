package de.tum.i13.simulator;

import org.apache.logging.log4j.ThreadContext;

public class SimulatorUtils {

    private SimulatorUtils() {
    }

    public static Runnable wrapWarnLogging(Runnable runnable) {
        return () -> {
            ThreadContext.put("SIMULATOR", "true");
            runnable.run();
        };
    }

}
