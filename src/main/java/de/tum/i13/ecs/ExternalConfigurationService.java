package de.tum.i13.ecs;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class responsible to maintain and update metadata of the servers.
 */
class ExternalConfigurationService {

    private static final Logger LOGGER = LogManager.getLogger(ExternalConfigurationService.class);


    private static final TreeMapServerMetadata serverMap = new TreeMapServerMetadata();

    private ExternalConfigurationService() {
    }

    /**
     * Add a new server to the {@link ConsistentHashRing} parameter. Prompt successor to hand off the relevant
     * key-value pairs to the new server in a {@link ECSHandoffThread}.
     * Update metadata of all other servers.
     *
     * @param listenAddress the address of the server to be added to the {@link ConsistentHashRing} parameter.
     * @param port          the port of the server to be added to the {@link ConsistentHashRing} parameter.
     */
    static synchronized void addServer(String listenAddress, int port) throws ECSException {
        LOGGER.info("Trying to add  new server with address '{}:{}' to the {}", listenAddress, port,
                ConsistentHashRing.class.getSimpleName());
        try {
            NetworkLocation newServer = new NetworkLocationImpl(listenAddress, port);
            if (!serverMap.isEmpty())
                handleHandoff(newServer);

            serverMap.addNetworkLocation(newServer);
            LOGGER.debug("Added new server '{}' to the {}", newServer, ConsistentHashRing.class.getSimpleName());
            updateMetadata();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to connect to '{}:{}' and create Handoff Thread.", listenAddress, port);
        }

    }

    private static synchronized void handleHandoff(NetworkLocation newServer) throws ECSException, IOException {
        LOGGER.trace("The {} is not empty. Handling handoff", ConsistentHashRing.class.getSimpleName());
        NetworkLocation previousInRing = serverMap.getPrecedingNetworkLocation(newServer)
                .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not " +
                        "find preceding server on ring"));
        NetworkLocation nextInRing = serverMap.getSucceedingNetworkLocation(newServer)
                .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not " +
                        "find succeeding server on ring"));

        BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing)
                .add(BigInteger.ONE);
        BigInteger upperBound = serverMap.getHashingAlgorithm().hash(newServer);

        //calculate the updated metadata and send it to both servers
        TreeMapServerMetadata copyMetadata = new TreeMapServerMetadata(serverMap);
        copyMetadata.addNetworkLocation(newServer);
        new ECSUpdateMetadataThread(newServer, copyMetadata.packMessage()).run();
        
        LOGGER.debug("Initiating Handoff between '{}' and '{}'", newServer, nextInRing);
        new ECSHandoffThread(nextInRing, newServer, lowerBound, upperBound, copyMetadata.packMessage()).run();
        // new ECSUpdateMetadataThread(nextInRing, copyMetadata.packMessage()).run();
    }

    /**
     * Remove server from the serverMap after it unexpectedly shuts down/fails to send back a HEARTBEAT.
     * Update metadata of all other servers. The key-value pairs of the failed server are considered lost.
     *
     * @param listenAddress the address of the server to be deleted from the {@link ConsistentHashRing},
     * @param port          the port of the server to be deleted from the {@link ConsistentHashRing}.
     */
    static synchronized void removeServer(String listenAddress, int port) {

        LOGGER.info("Trying to remove server with address {} from the ring.", listenAddress);

        NetworkLocationImpl serverLocation = new NetworkLocationImpl(listenAddress, port);

        //check if server already removed gracefully (eg by SERVER_SHUTDOWN message)
        if (serverMap.contains(serverLocation)) {
            serverMap.removeNetworkLocation(serverLocation);
            updateMetadata();
        }
    }

    /**
     * Remove server form the serverMap after sending its key-value pairs to its successor.
     * Update metadata of all other servers.
     *
     * @param listenAddress the address of the server to be deleted from the serverMap
     * @param port          the port of the server to be deleted from the serverMap
     */
    static synchronized void removeServerAndHandoffData(String listenAddress, int port) throws ECSException {
        LOGGER.info("Trying to remove server with address {} from the ring.", listenAddress);
        try {
            //Initiate handoff from old server to successor
            NetworkLocation oldServer = new NetworkLocationImpl(listenAddress, port);
            LOGGER.trace("Determining the predecessor and successor of {}", oldServer);
            NetworkLocation previousInRing = serverMap.getPrecedingNetworkLocation(oldServer)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "preceding server on ring"));
            NetworkLocation nextInRing = serverMap.getSucceedingNetworkLocation(oldServer)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "succeeding server on ring"));

            BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing);
            BigInteger upperBound = serverMap.getHashingAlgorithm().hash(oldServer);

            //calculate the updated metadata and send it to both servers
            TreeMapServerMetadata copyMetadata = new TreeMapServerMetadata(serverMap);
            copyMetadata.removeNetworkLocation(oldServer);
            new ECSUpdateMetadataThread(nextInRing, copyMetadata.packMessage()).run();

            LOGGER.debug("Initiating Handoff between from old server {} to successor {}", oldServer, nextInRing);
            new ECSHandoffThread(oldServer, nextInRing, lowerBound, upperBound, copyMetadata.packMessage()).run();

            //actually update the metadata
            serverMap.removeNetworkLocation(new NetworkLocationImpl(listenAddress, port));
            LOGGER.debug("Removed server '{}' from the ring", oldServer);
            updateMetadata();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to connect to '{}:{}' and create handoff thread", listenAddress, port);
        } catch (ECSException ex) {
            LOGGER.fatal("Handoff initiated by '{}:{}' failed.", listenAddress, port);
        }
    }

    private static void updateMetadata() {
        LOGGER.info("Trying to update the metadata of all servers in the ring.");
        try {
            ExecutorService executor = Executors.newCachedThreadPool();

            //prepare the metadata information in String format
            String metadata = serverMap.packMessage();

            for (NetworkLocation location : serverMap.getAllNetworkLocations())
                executor.submit(new ECSUpdateMetadataThread(location, metadata));

            executor.shutdown();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to create update metadata thread", ex);
        }
    }

}
