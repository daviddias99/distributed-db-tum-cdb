package de.tum.i13.ecs;

import de.tum.i13.server.Config;
import de.tum.i13.shared.Constants;
import static de.tum.i13.shared.LogSetup.setupLogging;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class that is responsible to listen for connections from new server that will join the ring.
 */
public class ExternalConfigurationServer {

    private static final Logger LOGGER = LogManager.getLogger(ExternalConfigurationServer.class);

    public static void main(String[] args){
        Config cfg = Config.parseCommandlineArgs(args);
        setupLogging(cfg.logfile, cfg.logLevel);

        try (final ServerSocket socket = new ServerSocket()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing ECS socket");
                try {
                    //TODO what else when ECS shuts down?
                    socket.close();
                } catch (IOException ex) {
                    LOGGER.fatal("Caught exception, while closing ECS socket", ex);
                }
            }));

            socket.bind(new InetSocketAddress(cfg.listenAddress, cfg.port));
        
            final ExternalConfigurationService service = new ExternalConfigurationService(cfg.listenAddress, cfg.port);
            startListening(socket, service);

        } catch(IOException ex){
            LOGGER.fatal("Caught exception, while creating and binding ECS socket", ex);
        }

    }

    public static void startListening(ServerSocket socket, ExternalConfigurationService service){

        ExecutorService executor = Executors.newFixedThreadPool(Constants.SERVER_POOL_SIZE);

        try {
            while (true) {
                // accept a connection from a new server
                Socket serverSocket = socket.accept();

                // start a new Thread for this connection
                executor.submit(new ECSServerThread(new ECSCommandProcessor(service), serverSocket));
            }
        } catch (IOException ex) {
            LOGGER.fatal("Caught exception while accepting server requests for ECS", ex);
            LOGGER.info("Closing ECS executor service");
            executor.shutdown();
        }
    }
    
}
