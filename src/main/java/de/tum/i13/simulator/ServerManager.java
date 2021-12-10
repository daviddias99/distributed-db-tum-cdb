package de.tum.i13.simulator;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;

public class ServerManager {

  LinkedList<Process> servers;
  LinkedList<String> addresses;
  int port = 25565;

  public ServerManager(int count) {

    this.servers = new LinkedList<>();
    this.addresses = new LinkedList<>();

    for (int i = 0; i < count; i++) {
      this.addServer();
    }

    Thread turnoffServersHook = new Thread(() -> {
      System.out.println("Turning off services");
      for (Process process : servers) {
        process.destroy();
      }
    });
    Runtime.getRuntime().addShutdownHook(turnoffServersHook);
  }

  private String getServerCommand() {
    String dataDir = Paths.get("data", Integer.toString(port)).toString();
    return String.format("java -jar target/kv-server.jar -p %d -d %s -l logs/server_%d.txt", this.port, dataDir, this.port);
  }

  public void addServer() {
    try {
      servers.addLast(Runtime.getRuntime().exec(this.getServerCommand()));
      this.addresses.push(String.format("127.0.0.1 %d", port));
      port++;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
