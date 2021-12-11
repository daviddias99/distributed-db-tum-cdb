package de.tum.i13.simulator;

public class DelayedEvent implements Runnable {

  enum Type {
    START_SERVER,
    STOP_SERVER
  }

  private int timeSeconds;
  private Type eType;
  private ServerManager manager;

  public DelayedEvent(int timeSeconds, Type eType, ServerManager manager) {

    this.eType = eType;
    this.timeSeconds = timeSeconds;
    this.manager = manager;
  }

  @Override
  public void run() {
    try {
      Thread.sleep((long) timeSeconds * 1000);

      switch (this.eType) {
        case START_SERVER:
          manager.addServer();
          break;
        case STOP_SERVER:
          manager.stopServer();
          break;
        default:
          break;
      }

    } catch (InterruptedException e) {
      System.out.println("Interrupted delayed event");
      Thread.currentThread().interrupt();
    }
  }
}
