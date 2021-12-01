package de.tum.i13.shared;

/**
 * Represents a location somewhere on the network with a host address anda a
 * port.
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

    // TODO: comment
    static NetworkLocation extractNetworkLocation(String networkLocationString) {
        final String[] networkLocationData = networkLocationString.split(":");
        if (networkLocationData.length != 2) {
            throw new IllegalArgumentException();
        }
        final String address = networkLocationData[0];
        final String port = networkLocationData[1];

        return new NetworkLocationImpl(address, Integer.parseInt(port));
    }
}
