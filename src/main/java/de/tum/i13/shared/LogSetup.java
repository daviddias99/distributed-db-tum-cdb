package de.tum.i13.shared;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.logging.*;

public class LogSetup {

    private static FileHandler createLogFile(String logFile) {
        FileHandler fileHandler = null;

        try {
            File directory = new File(Constants.LOGS_DIR);
            if (!directory.exists()){
                directory.mkdir();
            }

            fileHandler = new FileHandler(Paths.get(Constants.LOGS_DIR, logFile).toString(), true);        
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileHandler;
    }

    public static void setupLogging(String logfile) {
        Logger logger = LogManager.getLogManager().getLogger("");
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");

        FileHandler fileHandler = LogSetup.createLogFile(logfile);

        fileHandler.setFormatter(new SimpleFormatter());
        logger.addHandler(fileHandler);

        for (Handler h : logger.getHandlers()) {
            h.setLevel(Level.ALL);
        }
        logger.setLevel(Level.ALL); // we want log everything
    }
}
