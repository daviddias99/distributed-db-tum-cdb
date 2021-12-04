package de.tum.i13.shared.net;

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

}
