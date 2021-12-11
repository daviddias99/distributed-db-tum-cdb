package de.tum.i13.ecs;

import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.NetworkLocationImpl;

import java.math.BigInteger;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class responsible to maintain and update metadata of the servers.
 */
public class ExternalConfigurationService {
    
    private static final Logger LOGGER = LogManager.getLogger(ExternalConfigurationService.class);

    private final String address;
    private final int port;
    private final TreeMapServerMetadata serverMap;

    public ExternalConfigurationService(String address, int port){
        this.address = address;
        this.port = port;
        this.serverMap = new TreeMapServerMetadata();
    }

    /**
     * Getter method for the address of the ECS Service.
     * @return
     */
    public String getAddress(){
        return this.address;
    }

    /**
     * Getter method for the port of the ECS Service.
     * @return
     */
    public int getPort(){
        return this.port;
    }

    /**
     * Getter method for the metadata of the ECS Service.
     * @return
     */
    public ConsistentHashRing getMetadata(){
        return this.serverMap;
    }

    /**
     * Add a new server to the {@link ConsistentHashRing} parameter. Prompt successor to hand off the relevant 
     * key-value pairs to the new server in a {@link ECSHandoffThread}.
     * Update metadata of all other servers.
     * @param listenAddress the address of the server to be added to the {@link ConsistentHashRing} parameter.
     * @param port the port of the server to be added to the {@link ConsistentHashRing} parameter.
     */
    public synchronized void addServer(String listenAddress, int port){

        LOGGER.info("Trying to add a new server with address {} to the HashRing.", listenAddress);
        NetworkLocation newServer = new NetworkLocationImpl(listenAddress, port);
        NetworkLocation previousInRing = serverMap.getPreviousNetworkLocation(newServer);
        NetworkLocation nextInRing = serverMap.getNextNetworkLocation(newServer);

        BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing);
        BigInteger upperBound = serverMap.getHashingAlgorithm().hash(nextInRing);
        
        LOGGER.info("Initiating Handoff between " + listenAddress + " and " + nextInRing.getAddress());
        new ECSHandoffThread(nextInRing, newServer, lowerBound, upperBound).run();
        //What happens if HANDOFF fails?

        serverMap.addNetworkLocation(new NetworkLocationImpl(listenAddress, port));
        LOGGER.info("Added new server with address {} to the HashRing.", listenAddress);
        updateMetadata();
    }

    /**
     * Remove server from the serverMap after it unexpectedly shuts down/fails to send back a HEARTBEAT.
     * Update metadata of all other servers. The key-value pairs of the failed server are considered lost.
     * @param listenAddress the address of the server to be deleted from the {@link ConsistentHashRing},
     * @param port the port of the server to be deleted from the {@link ConsistentHashRing}.
     */
    public synchronized void removeServer(String listenAddress, int port){
        LOGGER.info("Trying to remove server with address {} from the ring.", listenAddress);

        NetworkLocationImpl serverLocation = new NetworkLocationImpl(listenAddress, port);

        //check if server already removed gracefully (eg by SERVER_SHUTDOWN message)
        if(serverMap.contains(serverLocation)){
            serverMap.removeNetworkLocation( serverLocation);
            updateMetadata();
        }
    }

    /**
     * Remove server form the serverMap after sending its key-value pairs to its successor.
     * Update metadata of all other servers.
     * @param listenAddress the address of the server to be deleted from the serverMap
     * @param port the port of the server to be deleted from the serverMap
     */
    public synchronized void removeServerAndHandoffData(String listenAddress, int port){

        LOGGER.info("Trying to remove server with address {} from the ring.", listenAddress);

        //Initiate handoff from old server to successor
        NetworkLocation oldServer = new NetworkLocationImpl(listenAddress, port);
        NetworkLocation previousInRing = serverMap.getPreviousNetworkLocation(oldServer);
        NetworkLocation nextInRing = serverMap.getNextNetworkLocation(oldServer);

        BigInteger lowerBound = serverMap.getHashingAlgorithm().hash(previousInRing);
        BigInteger upperBound = serverMap.getHashingAlgorithm().hash(oldServer);

        LOGGER.info("Initiating Handoff between " + listenAddress + " and " + nextInRing.getAddress());
        new ECSHandoffThread(oldServer, nextInRing, lowerBound, upperBound).run();

        serverMap.removeNetworkLocation(new NetworkLocationImpl(listenAddress, port));
        LOGGER.info("Removed server with address {} from the ring.", listenAddress);
        updateMetadata();
    }

    private void updateMetadata(){
        LOGGER.info("Trying to update the metadata of all servers in the ring.");
        ExecutorService executor = Executors.newFixedThreadPool(Constants.SERVER_POOL_SIZE);
        
        //get the list of server to send the update message to
        NavigableMap<BigInteger, NetworkLocation> map = serverMap.getNavigableMap();

        //prepare the metadata information in String format
        String metadata = serverMap.packMessage();

        for(NetworkLocation location: map.values()){
            executor.submit( new ECSUpdateMetadataThread(location, metadata));
        }

        executor.shutdown();
    }

}
