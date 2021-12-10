package de.tum.i13.simulator;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedList;
import picocli.CommandLine;
import de.tum.i13.client.shell.CLICommands;
import de.tum.i13.client.shell.ExitCodeMapper;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;

public class ClientSimulator implements Runnable {

  LinkedList<Pair<String>> toSend = new LinkedList<>();
  LinkedList<Pair<String>> sent = new LinkedList<>();;

  String serverAddress;
  int serverPort;

  public ClientSimulator(Path emailDir, String serverAddress, int serverPort) {

    File[] files = emailDir.toFile().listFiles();

    for (File file : files) {
      toSend.push(EmailParser.parseEmail(file.toPath()));
    }

    this.serverAddress = serverAddress;
    this.serverPort = serverPort;
  }

  @Override
  public void run() {
    final CLICommands commands = new CLICommands();
    final CommandLine cmd = new CommandLine(commands)
        .setExitCodeExceptionMapper(new ExitCodeMapper())
        .setCaseInsensitiveEnumValuesAllowed(true);

    cmd.execute(KVMessage.extractTokens("logLevel off"));
    cmd.execute(KVMessage.extractTokens(String.format("connect %s %d", serverAddress, serverPort)));
    cmd.execute(KVMessage.extractTokens(String.format("put %s %s", toSend.getFirst().key, toSend.getFirst().value)));
  }
}
