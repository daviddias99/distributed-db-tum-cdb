package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import de.tum.i13.server.Config;
import de.tum.i13.server.cache.CachedPersistentStorage;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.ChordException;
import de.tum.i13.server.kvchord.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.persistentstorage.btree.BTreePersistentStorage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.hashing.DebugHashAlgorithm;
import de.tum.i13.shared.hashing.MD5HashAlgorithm;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.server.state.ServerState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;


import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * The main class responsible for stating the database server.
 */
public class MainChord {

    private static final Logger LOGGER = LogManager.getLogger(MainChord.class);

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
            NetworkLocation boostrapLocation = cfg.bootstrap == null ? null : new NetworkLocationImpl(cfg.bootstrap.getAddress().getHostAddress(),
                    cfg.bootstrap.getPort());

            Chord chord = boostrapLocation == null ? 
                new Chord(new DebugHashAlgorithm(), curLocation) :
                new Chord(new DebugHashAlgorithm(), curLocation, boostrapLocation);

            // Create state
            LOGGER.trace("Creating server state");
            final ServerState state = new ServerState(curLocation);
            final CommandProcessor<String> commandProcessor = new KVCommandProcessor(storage, state, chord);

            LOGGER.trace("Starting the listening thread");
            // Listen for messages
            final Thread listeningThread = new Thread(new RequestListener(cfg.listenAddress, cfg.port, commandProcessor));

            // Setup shutdown procedure (handoff)
            // TODO: CHORD
            listeningThread.start();
        } catch (StorageException ex) {
            LOGGER.fatal("Caught exception while setting up storage", ex);
        } catch (ChordException e) {
            LOGGER.fatal("Could not start Chord instance");
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
}
