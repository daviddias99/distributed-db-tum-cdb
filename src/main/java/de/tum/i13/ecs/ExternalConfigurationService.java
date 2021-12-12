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
public class ExternalConfigurationService {

    private static final Logger LOGGER = LogManager.getLogger(ExternalConfigurationService.class);

    private final String address;
    private final int port;
    private final static TreeMapServerMetadata serverMap = new TreeMapServerMetadata();

    public ExternalConfigurationService(String address, int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * Getter method for the address of the ECS Service.
     *
     * @return
     */
    protected String getAddress() {
        return this.address;
    }

    /**
     * Getter method for the port of the ECS Service.
     *
     * @return
     */
    protected int getPort() {
        return this.port;
    }

    /**
     * Getter method for the metadata of the ECS Service.
     *
     * @return
     */
    protected static TreeMapServerMetadata getMetadata() {
        return serverMap;
    }

    /**
     * Add a new server to the {@link ConsistentHashRing} parameter. Prompt successor to hand off the relevant
     * key-value pairs to the new server in a {@link ECSHandoffThread}.
     * Update metadata of all other servers.
     *
     * @param listenAddress the address of the server to be added to the {@link ConsistentHashRing} parameter.
     * @param port          the port of the server to be added to the {@link ConsistentHashRing} parameter.
     */
    protected static synchronized void addServer(String listenAddress, int port) throws ECSException {

        try {
            LOGGER.info("Trying to add a new server with address {} to the HashRing.", listenAddress);
            NetworkLocation newServer = new NetworkLocationImpl(listenAddress, port);
            NetworkLocation previousInRing = serverMap.getPrecedingNetworkLocation(newServer)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "preceding server on ring"));
            NetworkLocation nextInRing = serverMap.getSucceedingNetworkLocation(newServer)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "succeeding server on ring"));

            BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing);
            BigInteger upperBound = serverMap.getHashingAlgorithm().hash(nextInRing);

            //calculate the updated metadata and send it to both servers
            TreeMapServerMetadata copyMetadata = new TreeMapServerMetadata(serverMap);
            copyMetadata.addNetworkLocation(newServer);
            new ECSUpdateMetadataThread(newServer, copyMetadata.packMessage());
            new ECSUpdateMetadataThread(nextInRing, copyMetadata.packMessage());

            LOGGER.info("Initiating Handoff between " + listenAddress + " and " + nextInRing.getAddress());
            new ECSHandoffThread(nextInRing, newServer, lowerBound, upperBound).run();

            //actually update the metadata
            serverMap.addNetworkLocation(new NetworkLocationImpl(listenAddress, port));

            LOGGER.info("Added new server with address {} to the HashRing.", listenAddress);
            updateMetadata();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to connect to " + listenAddress + " and create Handoff Thread.");
        }
    }

    /**
     * Remove server from the serverMap after it unexpectedly shuts down/fails to send back a HEARTBEAT.
     * Update metadata of all other servers. The key-value pairs of the failed server are considered lost.
     *
     * @param listenAddress the address of the server to be deleted from the {@link ConsistentHashRing},
     * @param port          the port of the server to be deleted from the {@link ConsistentHashRing}.
     */
    protected static synchronized void removeServer(String listenAddress, int port) {
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
    protected static synchronized void removeServerAndHandoffData(String listenAddress, int port) throws ECSException {

        LOGGER.info("Trying to remove server with address {} from the ring.", listenAddress);
        try {
            //Initiate handoff from old server to successor
            NetworkLocation serverToBeRemoved = new NetworkLocationImpl(listenAddress, port);
            NetworkLocation previousInRing = serverMap.getPrecedingNetworkLocation(serverToBeRemoved)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "preceding server on ring"));
            NetworkLocation nextInRing = serverMap.getSucceedingNetworkLocation(serverToBeRemoved)
                    .orElseThrow(() -> new ECSException(ECSException.Type.UPDATE_METADATA_FAILURE, "Could not find " +
                            "succeeding server on ring"));

            BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing);
            BigInteger upperBound = serverMap.getHashingAlgorithm().hash(serverToBeRemoved);

            //calculate the updated metadata and send it to successor
            TreeMapServerMetadata copyMetadata = new TreeMapServerMetadata(serverMap);
            copyMetadata.removeNetworkLocation(serverToBeRemoved);
            new ECSUpdateMetadataThread(nextInRing, copyMetadata.packMessage());

            LOGGER.info("Initiating Handoff between " + listenAddress + " and " + nextInRing.getAddress());
            new ECSHandoffThread(serverToBeRemoved, nextInRing, lowerBound, upperBound).run();

            //actually update the metadata
            serverMap.removeNetworkLocation(new NetworkLocationImpl(listenAddress, port));
            LOGGER.info("Removed server with address {} from the ring.", listenAddress);
            
            updateMetadata();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to connect to " + listenAddress + " and create Handoff Thread.");
        } catch (ECSException ex){
            LOGGER.fatal("Handoff initiated by {} failed.", listenAddress);
        }
    }

    private static void updateMetadata() {
        try {

            LOGGER.info("Trying to update the metadata of all servers in the ring.");
            ExecutorService executor = Executors.newFixedThreadPool(Constants.SERVER_POOL_SIZE);

            //prepare the metadata information in String format
            String metadata = serverMap.packMessage();

            for (NetworkLocation location : serverMap.getAllNetworkLocations()) {
                executor.submit(new ECSUpdateMetadataThread(location, metadata));
            }

            executor.shutdown();
        } catch (IOException ex) {
            LOGGER.fatal("Unable to create Update Metadata Thread.");
        }
    }

}
