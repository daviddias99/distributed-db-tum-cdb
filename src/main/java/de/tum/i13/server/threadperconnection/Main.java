package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import de.tum.i13.shared.net.NetworkMessageServer;
import de.tum.i13.server.Config;
import de.tum.i13.server.cache.CachedPersistentStorage;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.kv.commandprocessing.KVEcsCommandProcessor;
import de.tum.i13.server.kv.commandprocessing.handlers.ShutdownHandler;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.btree.BTreePersistentStorage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.server.state.ServerState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;


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

        try {
            // Setup storage
            final PersistentStorage storage = setUpStorage(cfg.dataDir, cfg.minimumDegree, cfg.cachingStrategy,
                    cfg.cacheSize);

            // TODO: if listenAddress is default (localhost, it won't correspond to the
            // correct metadata)
            NetworkLocation curLocation = new NetworkLocationImpl(cfg.listenAddress, cfg.port);
            NetworkLocation ecsLocation = new NetworkLocationImpl(cfg.bootstrap.getAddress().getHostAddress(),
                    cfg.bootstrap.getPort());

            // Create state
            LOGGER.trace("Creating server state");
            final ServerState state = new ServerState(curLocation, ecsLocation);

            // Setup communications with ECS
            final ServerCommunicator ecsCommunicator = setupEcsOutgoingCommunications(ecsLocation);
            final KVEcsCommandProcessor ecsCommandProcessor = new KVEcsCommandProcessor(storage, state, ecsCommunicator,
                    false);

            // Setup shutdown procedure (handoff)


            final CommandProcessor<String> commandProcessor = new KVCommandProcessor(storage, state, ecsCommunicator);

            LOGGER.trace("Starting the listening thread");
            // Listen for messages
            final Thread listeningThread = new Thread(new RequestListener(cfg.listenAddress, cfg.port, commandProcessor));
            LOGGER.trace("Adding shutdown handler for handoff");
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHandler(ecsCommunicator, ecsCommandProcessor, cfg, listeningThread)));
            listeningThread.start();
            LOGGER.trace("Waiting briefly until server is ready to accept new connections");
            Thread.sleep(500);

            // Request metadata from ECS
            LOGGER.info("Requesting metadata do ECS");
            ecsCommandProcessor.process(ecsCommunicator.signalStart(cfg.listenAddress, Integer.toString(cfg.port)));

        } catch (StorageException ex) {
            LOGGER.fatal("Caught exception while setting up storage", ex);
        } catch (CommunicationClientException ex) {
            LOGGER.fatal("Caught exception while connecting to the ECS", ex);
        } catch (InterruptedException exception) {
            LOGGER.warn("The current thread was interrupted", exception);
            Thread.currentThread().interrupt();
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
    private static CachedPersistentStorage setUpStorage(Path dataDir, int minimumDegree,
            CachingStrategy cachingStrategy, int cacheSize)
            throws StorageException {
        LOGGER.info("Setting up persistent storage at {}", dataDir);
        PersistentBTreeDiskStorageHandler<Pair<String>> handler = new PersistentBTreeDiskStorageHandler<>(
                dataDir.toString(),
                false);

        // TODO: is using MD5 by default, should somehow be configured with the one used
        // in the Ring
        BTreePersistentStorage storage = new BTreePersistentStorage(minimumDegree, handler);
        return new CachedPersistentStorage(storage, cachingStrategy, cacheSize);
    }

    private static ServerCommunicator setupEcsOutgoingCommunications(NetworkLocation ecsLocation)
            throws CommunicationClientException {
        LOGGER.info("Setting up outgoing communications to ECS");

        NetworkMessageServer messageServer = new CommunicationClient();
        ServerCommunicator communicator = new ServerCommunicator(messageServer);

        // Connect to ecs
        communicator.connect(ecsLocation.getAddress(), ecsLocation.getPort());
        return communicator;
    }
}
