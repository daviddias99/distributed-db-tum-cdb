package de.tum.i13.simulator;

public class DelayedEvent implements Runnable {

  public enum Type {
    START_SERVER,
    STOP_SERVER,
    START_CLIENT
  }

  private int timeSeconds;
  private Type eType;
  private ServerManager sManager;
  private ClientManager cManager;

  public DelayedEvent(int timeSeconds, Type eType, ServerManager sManager, ClientManager cManager) {

    this.eType = eType;
    this.timeSeconds = timeSeconds;
    this.sManager = sManager;
    this.cManager = cManager;
  }

  @Override
  public void run() {
    try {
      Thread.sleep((long) timeSeconds * 1000);

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
}
