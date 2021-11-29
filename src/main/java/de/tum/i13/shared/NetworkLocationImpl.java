package de.tum.i13.shared;

public class NetworkLocationImpl implements NetworkLocation {

    private final String address;
    private final int port;

    public NetworkLocationImpl(String address, int port) {
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
