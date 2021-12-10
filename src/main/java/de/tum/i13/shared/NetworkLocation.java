package de.tum.i13.shared;

/**
 * Represents a location somewhere on the network with a host address anda a port.
 */
public interface NetworkLocation {

    /**
     * Returns the host address of the network location.
     *
     * @return the address of the network location
     */
    String getAddress();

    /**
     * Returns the port of the network location.
     *
     * @return the port of the network location
     */
    int getPort();

    /**
     * Create a String from a {@link NetworkLocation} object.
     * @param location the {@link NetworkLocation} object to be converted to String.
     * @return a String representation of the network location.
     */
    static String packNetworkLocation(NetworkLocation location){
        return location.getAddress() + ":" + location.getPort();
    }

}
