package de.tum.i13.simulator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Random;

import picocli.CommandLine;
import de.tum.i13.client.shell.CLICommands;
import de.tum.i13.client.shell.ExitCodeMapper;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;

public class ClientSimulator implements Runnable {

  LinkedList<Pair<String>> toSend = new LinkedList<>();
  LinkedList<Pair<String>> sent = new LinkedList<>();

  int totalEmailCount;
  String serverAddress;
  int serverPort;
  boolean stop = false;

  CommandLine cmd;

  ClientStats stats;

  private static final double NANOS_PER_SECOND = 1000000000;

  public ClientSimulator(Path emailDir, String serverAddress, int serverPort) {
    this.serverAddress = serverAddress;
    this.serverPort = serverPort;
    this.stats = new ClientStats();

    this.setupEmails(emailDir);
  }

  private void setupEmails(Path emailDir) {
    File[] files = emailDir.toFile().listFiles();

    for (File file : files) {
      Pair<String> parsedEmail = EmailParser.parseEmail(file.toPath());

      if (parsedEmail != null) {
        toSend.push(EmailParser.parseEmail(file.toPath()));
      }
    }
    this.totalEmailCount = files.length;
  }

  private void setupConnection() {
    try (PrintWriter temp = new PrintWriter("temp.txt")) {
      final CLICommands commands = new CLICommands();
      cmd = new CommandLine(commands)
          .setOut(temp)
          .setErr(temp)
          .setExitCodeExceptionMapper(new ExitCodeMapper())
          .setCaseInsensitiveEnumValuesAllowed(true);
      int exitCode = cmd.execute(KVMessage.extractTokens("logLevel off"));
      exitCode = cmd.execute(KVMessage.extractTokens(String.format("connect %s %d", serverAddress, serverPort)));

      if (exitCode != 20) {
        Thread.currentThread().interrupt();
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void getRandom() {
    if (this.sent.isEmpty()) {
      return;
    }

    Random rand = new Random();
    int toGetIndex = rand.nextInt(this.sent.size());
    Pair<String> email = this.sent.get(toGetIndex);

    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("get %s", email.key)));
    long time2 = System.nanoTime();

    boolean fail = false;

    if (exitCode != 20) {
      fail = true;
      sent.remove(toGetIndex);
    }

    synchronized (this.stats) {
      this.stats.get((time2 - time1) / NANOS_PER_SECOND, fail);
    }
  }

  private void putRandom() {
    if (this.toSend.isEmpty()) {
      return;
    }

    Random rand = new Random();
    int toSendIndex = rand.nextInt(this.toSend.size());
    Pair<String> email = this.toSend.get(toSendIndex);

    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s %s", email.key, email.value.substring(0, Math.min(email.value.length(), 200)))));
    long time2 = System.nanoTime();

    boolean fail = false;

    if (exitCode != 20) {
      fail = true;
    } else {
      toSend.remove(toSendIndex);
      sent.addLast(email);
    }

    synchronized (this.stats) {
      this.stats.put((time2 - time1) / NANOS_PER_SECOND, fail);
    }
  }

  private void deleteRandom() {
    if (this.sent.size() < 0.3 * this.totalEmailCount) {
      return;
    }

    Random rand = new Random();
    int toDeleteIndex = rand.nextInt(this.sent.size());
    Pair<String> email = this.sent.get(toDeleteIndex);

    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s null", email.key)));
    long time2 = System.nanoTime();

    boolean fail = false;

    if (exitCode != 20) {
      fail = true;
    } else {
      sent.remove(toDeleteIndex);
      toSend.addLast(email);
    }

    synchronized (this.stats) {
      this.stats.delete((time2 - time1) / NANOS_PER_SECOND, fail);
    }
  }

  @Override
  public void run() {
    this.setupConnection();

    while (!Thread.interrupted()) {
      if (!this.sent.isEmpty()) {
        this.getRandom();
      }

      if (this.sent.size() >= 0.3 * this.totalEmailCount) {
        if (Math.random() > 0.5) {
          this.deleteRandom();
        } else {
          this.putRandom();
        }
      } else {
        this.putRandom();
      }
    }
  }
}
