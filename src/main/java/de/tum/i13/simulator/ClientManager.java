package de.tum.i13.simulator;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Random;

public class ClientManager {

  LinkedList<Thread> clientThreads;
  LinkedList<ClientSimulator> clients;
  File[] emailDirs;
  int counter = 0;
  int clientCount;
  ServerManager servers;

  public ClientManager(int count, ServerManager servers) {
    this.servers = servers;
    emailDirs = Paths.get("maildir").toFile().listFiles();

    this.clientThreads = new LinkedList<>();
    this.clients = new LinkedList<>();
    this.clientCount = count;
  }

  public void startClients() {
    for (int i = 0; i < clientCount; i++) {

      Path path = null;

      do {
        path = Paths.get(emailDirs[counter++].getAbsolutePath(), "all_documents");
      } while (!path.toFile().exists() || path.toFile().listFiles().length < 15);

      this.addClient(path);
    }
    this.countStatistics();
  }

  private void countStatistics() {
    (new Thread(new StatsAccumulator(clients))).start();
  }

  public void addClient(Path emailsPath) {
    Random random = new Random();
    int serverIndex = random.nextInt(servers.servers.size());
    int port = Integer.parseInt(servers.addresses.get(serverIndex).split(" ")[1]);
    ClientSimulator newClient = new ClientSimulator(emailsPath, "127.0.0.1", port);
    this.clients.add(newClient);
    Thread clientThread = new Thread(newClient);
    this.clientThreads.add(clientThread);
    this.clientThreads.getFirst().start();
  }
}
