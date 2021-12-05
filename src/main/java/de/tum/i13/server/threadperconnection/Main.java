package de.tum.i13.server.threadperconnection;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.CommunicationClient;
import de.tum.i13.client.net.NetworkMessageServer;
import de.tum.i13.server.Config;
import de.tum.i13.server.cache.CachedPersistentStorage;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.kv.KVConnectionHandler;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.kv.commandprocessing.KVEcsCommandProcessor;
import de.tum.i13.server.kv.commandprocessing.ShutdownHandler;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.persistentstorage.btree.BTreePersistentStorage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.NetworkLocationImpl;

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
     * Starts the database server with the configured arguments or default values
     * from {@link Config}
     *
     * @param args the command line arguments as specified by {@link Config}
     */
    public static void main(String[] args) {
        Config cfg = Config.parseCommandlineArgs(args); // Do not change this
        setupLogging(cfg.logfile, cfg.logLevel);

        try (final ServerSocket serverSocket = new ServerSocket()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Closing server socket");
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                    LOGGER.fatal("Caught exception, while closing server socket", ex);
                }
            }));

            // bind to localhost only
            serverSocket.bind(new InetSocketAddress(cfg.listenAddress, cfg.port));

            // Setup storage
            final PersistentStorage storage = setUpStorage(cfg.dataDir, cfg.cachingStrategy, cfg.cacheSize);

            // TODO: if listenAddress is default (localhost, it won't correspond to the
            // correct metadata)
            NetworkLocation curLocation = new NetworkLocationImpl(cfg.listenAddress, cfg.port);
            NetworkLocation ecsLocation = new NetworkLocationImpl(cfg.bootstrap.getAddress().getHostAddress(),
                    cfg.bootstrap.getPort());

            // Create state        
            final ServerState state = new ServerState(curLocation, ecsLocation);

            // Setup communications with ECS
            final ServerCommunicator ecsCommunicator = setupEcsOutgoingCommunications(ecsLocation);
            final KVEcsCommandProcessor ecsCommandProcessor = new KVEcsCommandProcessor(storage, state, ecsCommunicator, false);
            
            // Setup shutdown procedure (handoff)
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHandler(ecsCommunicator, ecsCommandProcessor)));
            
            // Request metadata from ECS
            ecsCommandProcessor.process(ecsCommunicator.requestMetadata(), PeerType.ECS);

            final CommandProcessor<String> commandProcessor = new KVCommandProcessor(storage, state, ecsCommunicator);

            // Listen for messages
            startListening(serverSocket, storage, state, commandProcessor);

        } catch (IOException ex) {
            LOGGER.fatal("Caught exception, while creating and binding server socket", ex);
        } catch (StorageException ex) {
            LOGGER.fatal("Caught exception while setting up storage", ex);
        } catch (ClientException ex) {
            LOGGER.fatal("Caught exception while connecting to the ECS", ex);
        }
    }

    /**
     * Method that sets the persistent storage directory, caching strategy and cache
     * size.
     * 
     * @param dataDir
     * @param cachingStrategy
     * @param cacheSize
     * @return
     */
    private static CachedPersistentStorage setUpStorage(Path dataDir, CachingStrategy cachingStrategy, int cacheSize)
            throws StorageException {
        PersistentBTreeDiskStorageHandler<Pair<String>> handler = new PersistentBTreeDiskStorageHandler<>(
                dataDir.toString(),
                false);

        // TODO: is using MD5 by default, should somehow be configured with the one used
        // in the Ring
        BTreePersistentStorage storage = new BTreePersistentStorage(3, handler);
        return new CachedPersistentStorage(storage, cachingStrategy, cacheSize);
    }

    private static ServerCommunicator setupEcsOutgoingCommunications(NetworkLocation ecsLocation)
            throws ClientException {
        NetworkMessageServer messageServer = new CommunicationClient();
        ServerCommunicator communicator = new ServerCommunicator(messageServer);

        // Connect to ecs
        communicator.connect(ecsLocation.getAddress(), ecsLocation.getPort());
        return communicator;
    }

    @SuppressWarnings("java:S2189")
    private static void startListening(ServerSocket serverSocket, PersistentStorage storage, ServerState state,
            CommandProcessor<String> commandProcessor)
            throws ClientException {
        // Replace with your Key value server logic.
        // If you use multithreading you need locking

        ConnectionHandler cHandler = new KVConnectionHandler();

        // Use ThreadPool
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.CORE_POOL_SIZE);

        try {
            while (true) {
                // accept a connection
                Socket clientSocket = serverSocket.accept();

                // start a new Thread for this connection
                executorService.submit(new ConnectionHandleThread(commandProcessor, cHandler, state, clientSocket,
                        (InetSocketAddress) serverSocket.getLocalSocketAddress()));
            }
        } catch (IOException ex) {
            LOGGER.fatal("Caught exception while accepting client request", ex);
            LOGGER.info("Closing executor service");
            executorService.shutdown();
        }
    }

}
