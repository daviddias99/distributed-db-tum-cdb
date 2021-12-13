package de.tum.i13.simulator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import de.tum.i13.server.persistentstorage.btree.chunk.Pair;

public class EmailParser {

  private EmailParser() {
  }

  public static Pair<String> parseEmail(Path emailPath) {
    try {
      String content = Files.readString(emailPath, StandardCharsets.US_ASCII).replaceAll("[\\t\\n\\r]+"," ");
      return new Pair<>(getIdFromEmail(content), content);
    } catch (IOException e) {
      // e.printStackTrace();
    }

    return null;
  }

  private static String getIdFromEmail(String email) {
    return email.substring("Message-ID: <13668446.".length(), email.indexOf(".JavaMail"));
  }
}