package de.tum.i13.server.nio;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.server.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartSimpleNioServer {

    private static final Logger LOGGER = LogManager.getLogger(StartSimpleNioServer.class);

    public static void main(String[] args) throws IOException {
        Config cfg = Config.parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        LOGGER.info("Config: {}", cfg);

        LOGGER.info("starting server");

        //Replace with your Key Value command processor
        CommandProcessor echoLogic = new EchoLogic();

        SimpleNioServer sn = new SimpleNioServer(echoLogic);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        sn.start();
    }
}
