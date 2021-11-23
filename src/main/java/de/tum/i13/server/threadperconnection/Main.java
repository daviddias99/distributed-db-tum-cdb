package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.Config;
import de.tum.i13.server.cache.CachedPersistentStorage;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.persistentStorage.btree.BTreePersistentStorage;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.StorageException;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
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

            //set up storage options
            final PersistentStorage storage = setUpStorage(cfg.dataDir, cfg.cachingStrategy, cfg.cacheSize);

            //start server
            startListening(serverSocket, storage);

        } catch (IOException ex) {
            LOGGER.fatal("Caught exception, while creating and binding server socket", ex);
        } catch (StorageException ex) {
            LOGGER.fatal("Caught exception while setting up storage", ex);
        }
    }

    /**
     * Method that sets the persistent storage directory, caching strategy and cache size.
     * @param dataDir
     * @param cachingStrategy
     * @param cacheSize
     * @return
     */
    private static CachedPersistentStorage setUpStorage(Path dataDir, CachingStrategy cachingStrategy, int cacheSize) throws StorageException {
        PersistentBTreeDiskStorageHandler<String> handler = new PersistentBTreeDiskStorageHandler<>(dataDir.toString(), false);
        BTreePersistentStorage storage = new BTreePersistentStorage(3, handler);
        return new CachedPersistentStorage(storage, cachingStrategy, cacheSize);
    }

    @SuppressWarnings("java:S2189")
    private static void startListening(ServerSocket serverSocket, PersistentStorage storage) {
        //Replace with your Key value server logic.
        // If you use multithreading you need locking

        CommandProcessor logic = new KVCommandProcessor(storage);

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
