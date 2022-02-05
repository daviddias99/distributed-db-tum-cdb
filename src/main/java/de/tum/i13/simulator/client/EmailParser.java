package de.tum.i13.simulator.client;

import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmailParser {

    private static final Logger LOGGER = LogManager.getLogger(EmailParser.class);

    private EmailParser() {
    }

    public static Pair<String> parseEmail(Path emailPath) {
        try {
            String content = Files.readString(emailPath, StandardCharsets.US_ASCII).replaceAll("[\\t\\n\\r]+", " ");
            return new Pair<>(getIdFromEmail(content), content);
        } catch (IOException e) {
            LOGGER.error("Caught exception while parsing E-Mail", e);
        }

        return null;
    }

    private static String getIdFromEmail(String email) {
        return email.substring("Message-ID: <13668446.".length(), email.indexOf(".JavaMail"));
    }

}