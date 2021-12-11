package de.tum.i13.simulator;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;

public class ClientManager {

  LinkedList<Thread> clientThreads;
  LinkedList<ClientSimulator> clients;
  File[] emailDirs;
  int counter = 0;
  ServerManager servers;

  public ClientManager(int count) {

    // this.servers = servers;
    emailDirs = Paths.get("maildir").toFile().listFiles();


    this.clientThreads = new LinkedList<>();
    this.clients = new LinkedList<>();
    
    for (int i = 0; i < count; i++) {

      Path path = null;

      do {
        path = Paths.get(emailDirs[counter++].getAbsolutePath(), "all_documents");
      }while(!path.toFile().exists() || path.toFile().listFiles().length < 15);


      this.addClient(path);
    }
  }

  public void addClient(Path emailsPath) {
    // ClientSimulator newClient = new ClientSimulator(emailsPath, "127.0.0.1", Integer.parseInt(servers.addresses.get(0).split(" ")[1]) );
    ClientSimulator newClient = new ClientSimulator(emailsPath, "127.0.0.1",25565);
    this.clients.add(newClient);
    this.clientThreads.add(new Thread(newClient));
    this.clientThreads.getFirst().start();
  }
}
