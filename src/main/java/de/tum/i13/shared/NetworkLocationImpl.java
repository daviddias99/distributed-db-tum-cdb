package de.tum.i13.shared;

/**
 * Basic data class implementation of {@link NetworkLocation}
 */
public class NetworkLocationImpl implements NetworkLocation {

    private final String address;
    private final int port;

    /**
     * Creates a new {@link NetworkLocation} with the given address and port
     *
     * @param address the host address of the {@link NetworkLocation}, must not be null
     * @param port    the port of the {@link NetworkLocation}, must be between 0 and 65535 inclusive
     */
    public NetworkLocationImpl(String address, int port) {
        Preconditions.notNull(address, "Host address must not be null");
        Preconditions.check(
                port >= 0 && port <= 65535,
                "Port number must be between 0 and 65535 inclusive");

        this.address = address;
        this.port = port;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public int getPort() {
        return this.port;
    }

}