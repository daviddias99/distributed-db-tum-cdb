package de.tum.i13.simulator;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedList;

import de.tum.i13.server.persistentstorage.btree.chunk.Pair;

public class ClientSimulator implements Runnable {

  LinkedList<Pair<String>> toSend;
  LinkedList<Pair<String>> sent;

  public ClientSimulator(Path emailDir) {

    File[] files = emailDir.toFile().listFiles();

    for (File file : files) {
      toSend.push(EmailParser.parseEmail(file.toPath()));
    }
  }

  @Override
  public void run() {

  }
}
