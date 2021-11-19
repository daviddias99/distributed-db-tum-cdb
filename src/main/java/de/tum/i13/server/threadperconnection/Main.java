package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.Config;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreStub;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * The main class responsible for stating the database server.
 */
public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class);


    /**
     * Starts the database server with the configured arguments or default values from {@link Config}
     *
     * @param args the command line arguments as specified by {@link Config}
     */
    public static void main(String[] args) {
        Config cfg = Config.parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.logLevel);

        try (final ServerSocket serverSocket = new ServerSocket()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing server socket");
                System.out.println("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                    LOGGER.fatal("Caught exception, while closing server socket", ex);
                }
            }));

            //bind to localhost only
            serverSocket.bind(new InetSocketAddress(cfg.listenAddress, cfg.port));

            startListening(serverSocket);
        } catch (IOException ex) {
            LOGGER.fatal("Caught exception, while creating and binding server socket", ex);
        }
    }

    @SuppressWarnings("java:S2189")
    private static void startListening(ServerSocket serverSocket) {
        //Replace with your Key value server logic.
        // If you use multithreading you need locking
        CommandProcessor logic = new KVCommandProcessor(new KVStoreStub());

        //Use ThreadPool
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.CORE_POOL_SIZE);


        try{
            while (true) {
                //accept a connection
                Socket clientSocket = serverSocket.accept();

                //start a new Thread for this connection
                executorService.submit(new ConnectionHandleThread(logic, clientSocket, (InetSocketAddress) serverSocket.getLocalSocketAddress()));
            }
        } catch(IOException ex){
            LOGGER.fatal("Caught exception while accepting client request", ex);
            LOGGER.info("Closing executor service");
            executorService.shutdown();
        }
    }

}
