package de.tum.i13.simulator;

public class DelayedEvent implements Runnable, TimeEvent {

    private final int timeSeconds;
    private final Type eType;
    private final ServerManager sManager;
    private final ClientManager cManager;
    private final StatsAccumulator acc;
    public DelayedEvent(int timeSeconds, Type eType, ServerManager sManager, ClientManager cManager,
                        StatsAccumulator acc) {

        this.eType = eType;
        this.timeSeconds = timeSeconds;
        this.sManager = sManager;
        this.cManager = cManager;
        this.acc = acc;
    }

    @Override
    public void run() {
        try {
            Thread.sleep((long) timeSeconds * 1000);

            acc.signalEvent(this);
            switch (this.eType) {
                case START_SERVER:
                    sManager.addServer();
                    break;
                case STOP_SERVER:
                    sManager.stopServer();
                    break;
                case START_CLIENT:
                    cManager.addAndStartClient();
                    break;
                case STOP_PROGRAM:
                    System.exit(0);
                    break;
                default:
                    break;
            }

        } catch (InterruptedException e) {
            System.out.println("Interrupted delayed event");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toCSVString() {
        return this.eType.name() + ",,,,,,,,\n";
    }

    @Override
    public String toString() {
        return this.eType.name();
    }

    public enum Type {
        START_SERVER,
        STOP_SERVER,
        START_CLIENT,
        STOP_PROGRAM
    }

}
