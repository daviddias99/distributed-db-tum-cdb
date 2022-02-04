package de.tum.i13.simulator;

import org.apache.logging.log4j.ThreadContext;

public class WarnLoggingRunnable implements Runnable {

    private final Runnable runnable;

    public WarnLoggingRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public void run() {
        ThreadContext.put("SIMULATOR", "true");
        runnable.run();
    }

    public static WarnLoggingRunnable wrapWarnLogging(Runnable runnable) {
        return new WarnLoggingRunnable(runnable);
    }

}
