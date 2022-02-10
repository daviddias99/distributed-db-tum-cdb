package de.tum.i13.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SharedUtils {

    private static final Logger LOGGER = LogManager.getLogger(SharedUtils.class);

    private SharedUtils() {

    }

    public static Runnable withExceptionsLogged(Runnable function) {
        return () -> {
            try {
                function.run();
            } catch (Exception e) {
                LOGGER.atError()
                        .withThrowable(e)
                        .log("Exception was caught during periodic thread execution");
            }
        };
    }

}
