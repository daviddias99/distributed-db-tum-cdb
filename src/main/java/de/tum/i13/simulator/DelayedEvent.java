package de.tum.i13.simulator;

public class DelayedEvent implements Runnable, TimeEvent {

  public enum Type {
    START_SERVER,
    STOP_SERVER,
    START_CLIENT
  }

  private int timeSeconds;
  private Type eType;
  private ServerManager sManager;
  private ClientManager cManager;
  private StatsAccumulator acc;

  public DelayedEvent(int timeSeconds, Type eType, ServerManager sManager, ClientManager cManager, StatsAccumulator acc) {

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
}
