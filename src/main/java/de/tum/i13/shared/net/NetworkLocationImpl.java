package de.tum.i13.shared.net;

import de.tum.i13.shared.Preconditions;

import java.util.Objects;

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

    /**
     * Creates a new null {@link NetworkLocation} with address "null" and port 0
     */
    NetworkLocationImpl() {
        this.address = "null";
        this.port = 0;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) return true;
        if (!(otherObject instanceof NetworkLocation)) return false;
        NetworkLocation that = (NetworkLocation) otherObject;
        return getPort() == that.getPort() && Objects.equals(getAddress(), that.getAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAddress(), getPort());
    }

    @Override
    public String toString() {
        return String.format("%s{%s:%s}", NetworkLocationImpl.class.getSimpleName(), getAddress(), getPort());
    }

}
