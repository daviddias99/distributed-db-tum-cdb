package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.Config;
import de.tum.i13.server.cache.CachedPersistentStorage;
import de.tum.i13.server.cache.CachingStrategy;
import de.tum.i13.server.kvchord.Chord;
import de.tum.i13.server.kvchord.ChordException;
import de.tum.i13.server.kvchord.KVChordListener;
import de.tum.i13.server.kvchord.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.kvchord.commandprocessing.handlers.ShutdownHandler;
import de.tum.i13.server.persistentstorage.btree.BTreePersistentStorage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.MD5HashAlgorithm;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;

import static de.tum.i13.shared.LogSetup.setupLogging;
import static de.tum.i13.shared.SharedUtils.withExceptionsLogged;

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

        // TODO: Not really a good practice
        Constants.NUMBER_OF_REPLICAS = cfg.replicationFactor;

        try {
            // Setup storage
            final HashingAlgorithm hashingAlgorithm = new MD5HashAlgorithm();
            final PersistentStorage storage = setUpStorage(cfg.dataDir, cfg.minimumDegree, cfg.cachingStrategy,
                    cfg.cacheSize, hashingAlgorithm);

            // TODO: if listenAddress is default (localhost, it won't correspond to the
            // correct metadata)
            NetworkLocation curLocation = new NetworkLocationImpl(cfg.listenAddress, cfg.port);
            NetworkLocation boostrapLocation = cfg.bootstrap == null ? null :
                    new NetworkLocationImpl(cfg.bootstrap.getAddress().getHostAddress(),
                    cfg.bootstrap.getPort());

            Chord chord = boostrapLocation == null ?
                    new Chord(hashingAlgorithm, curLocation) :
                    new Chord(hashingAlgorithm, curLocation, boostrapLocation);

            // Create state
            LOGGER.trace("Creating server state");
            final ChordServerState state = new ChordServerState(chord);
            final CommandProcessor<String> commandProcessor = new KVCommandProcessor(storage, state, chord);

            KVChordListener chordListener = new KVChordListener(state, storage, hashingAlgorithm);
            chord.addListener(chordListener);

            LOGGER.trace("Starting the listening thread");
            // Listen for messages
            final Thread listeningThread = new Thread(withExceptionsLogged(new RequestListener(cfg.listenAddress,
                    cfg.port, commandProcessor)));

            // Setup shutdown procedure (handoff)
            Runtime.getRuntime().addShutdownHook(new Thread(withExceptionsLogged(new ShutdownHandler(listeningThread,
                    chord, state, storage, chordListener))));

            listeningThread.start();
            Thread.sleep(600);
            chord.start();
        } catch (StorageException ex) {
            LOGGER.fatal("Caught exception while setting up storage", ex);
        } catch (InterruptedException exception) {
            LOGGER.warn("The current thread was interrupted", exception);
            Thread.currentThread().interrupt();
        } catch (ChordException ex) {
            LOGGER.fatal("Could not start Chord", ex);
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
                                                        CachingStrategy cachingStrategy, int cacheSize,
                                                        HashingAlgorithm hashAlg)
            throws StorageException {
        LOGGER.info("Setting up persistent storage at {}", dataDir);
        PersistentBTreeDiskStorageHandler<Pair<String>> handler = new PersistentBTreeDiskStorageHandler<>(
                dataDir.toString(),
                false);

        BTreePersistentStorage storage = new BTreePersistentStorage(minimumDegree, handler, hashAlg);
        return new CachedPersistentStorage(storage, cachingStrategy, cacheSize);
    }

}
