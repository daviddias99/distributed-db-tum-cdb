package de.tum.i13.simulator;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;

import picocli.CommandLine;
import de.tum.i13.client.shell.CLICommands;
import de.tum.i13.client.shell.ExecutionExceptionHandler;
import de.tum.i13.client.shell.ExitCodeMapper;
import de.tum.i13.client.shell.ParameterExceptionHandler;

public class ClientManager {

  LinkedList<Thread> clientThreads;
  LinkedList<ClientSimulator> clients;


  public ClientManager(int count) {

    this.clientThreads = new LinkedList<>();
    this.clients = new LinkedList<>();

    for (int i = 0; i < count; i++) {
      this.addClient();
    }

    Thread turnoffServersHook = new Thread(() -> {
      for (Process process : servers) {
        process.destroy();
      }
    });
    Runtime.getRuntime().addShutdownHook(turnoffServersHook);
  }

  private String getServerCommand() {
    String dataDir = Paths.get("data", Integer.toString(port)).toString();
    return String.format("java -jar target/kv-server.jar -p %d -d %s", this.port, dataDir);
  }

  public void addClient() {
    try {
      final CLICommands commands = new CLICommands();
      final CommandLine cmd = new CommandLine(commands)
          .setExitCodeExceptionMapper(new ExitCodeMapper())
          .setCaseInsensitiveEnumValuesAllowed(true);
  
      cmd.execute("logLevel off".split(" "));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
