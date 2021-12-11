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

  long putTime = 0;
  long getTime = 0;
  long deleteTime = 0;
  int putCount = 0;
  int getCount = 0;
  int deleteCount = 0;
  int putFailCount = 0;
  int getFailCount = 0;
  int deleteFailCount = 0;
  int totalEmailCount;

  String serverAddress;
  int serverPort;

  CommandLine cmd;

  public ClientSimulator(Path emailDir, String serverAddress, int serverPort) {
    this.serverAddress = serverAddress;
    this.serverPort = serverPort;

    this.setupEmails(emailDir);
  }

  private void setupEmails(Path emailDir) {
    File[] files = emailDir.toFile().listFiles();

    for (File file : files) {
      toSend.push(EmailParser.parseEmail(file.toPath()));
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
      int exitCode = cmd.execute(KVMessage.extractTokens("logLevel error"));
      exitCode = cmd.execute(KVMessage.extractTokens(String.format("connect %s %d", serverAddress, serverPort)));

      if(exitCode != 20) {
        Thread.currentThread().interrupt();
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void getRandom() {
    Random rand = new Random();
    Pair<String> email = this.sent.get(rand.nextInt(this.sent.size()));
  
    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("get %s",email.key)));
    long time2 = System.nanoTime();

    getTime += time2 - time1;
    getCount++;

    if(exitCode != 20) {
      System.out.println(exitCode);
      getFailCount++;
    }
  }

  private void putRandom() {
    Random rand = new Random();
    int toSendIndex = rand.nextInt(this.toSend.size());
    Pair<String> email = this.toSend.get(toSendIndex);

    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s %s", email.key, email.value)));
    long time2 = System.nanoTime();

    putTime += time2 - time1;
    putCount++;

    if (exitCode != 20) {
      System.out.println(exitCode);
      putFailCount++;
    } else {
      toSend.remove(toSendIndex);
      sent.addLast(email);
    }
  }

  private void deleteRandom() {
    Random rand = new Random();
    int toDeleteIndex = rand.nextInt(this.sent.size());
    Pair<String> email = this.sent.get(toDeleteIndex);

    long time1 = System.nanoTime();
    int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s null", email.key)));
    long time2 = System.nanoTime();

    deleteTime += time2 - time1;
    deleteCount++;

    if (exitCode != 20) {
      System.out.println(exitCode);
      deleteFailCount++;
    } else {
      sent.remove(toDeleteIndex);
      toSend.addLast(email);
    }
  }

  @Override
  public void run() {
    this.setupConnection();

    while (true) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      this.print();
      if (this.sent.size() < 0.4 * this.totalEmailCount) {
        this.putRandom();
        continue;
      } 

      double random = Math.random();

      if(random <= 0.4) {
        this.getRandom();
      } else if (random <= 0.7) {
        this.putRandom();
      } else{
        this.deleteRandom();
      }


    }
  }

  private void print() {
    System.out.println(">> STATE");
    System.out.println(String.format("GET(%d, %d, %d)", getCount, getFailCount, getTime));
    System.out.println(String.format("PUT(%d, %d, %d)", putCount, putFailCount, putTime));
    System.out.println(String.format("DELETE(%d, %d, %d)", deleteCount, deleteFailCount, deleteTime));
  }
}
