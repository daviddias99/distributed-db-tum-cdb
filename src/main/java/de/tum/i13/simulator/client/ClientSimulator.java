package de.tum.i13.simulator.client;

import de.tum.i13.client.shell.CLICommands;
import de.tum.i13.client.shell.ExecutionExceptionHandler;
import de.tum.i13.client.shell.ExitCode;
import de.tum.i13.client.shell.ExitCodeMapper;
import de.tum.i13.client.shell.ParameterExceptionHandler;
import de.tum.i13.client.shell.ServerType;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.simulator.events.ClientStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Random;

public class ClientSimulator implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ClientSimulator.class);

    private static final double NANOS_PER_SECOND = 1000000000;
    public ClientStats stats;
    LinkedList<Pair<String>> toSend = new LinkedList<>();
    LinkedList<Pair<String>> sent = new LinkedList<>();
    int totalEmailCount;
    String serverAddress;
    int serverPort;
    boolean stop = false;
    CommandLine cmd;
    private final boolean useChord;

    public ClientSimulator(Path emailDir, String serverAddress, int serverPort, boolean useChord) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.stats = new ClientStats();
        this.useChord = useChord;

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
            final CLICommands commands = new CLICommands(this.useChord ? ServerType.CHORD : ServerType.ECS);
            cmd = new CommandLine(commands)
                    .setOut(temp)
                    .setErr(temp)
                    .setExitCodeExceptionMapper(new ExitCodeMapper())
                    .setParameterExceptionHandler(new ParameterExceptionHandler())
                    .setExecutionExceptionHandler(new ExecutionExceptionHandler())
                    .setCaseInsensitiveEnumValuesAllowed(true);
            int exitCode = cmd.execute(KVMessage.extractTokens(String.format("connect %s %d", serverAddress,
                    serverPort)));

            if (exitCode != ExitCode.SUCCESS.getValue()) {
                Thread.currentThread().interrupt();
            }

        } catch (FileNotFoundException e) {
            LOGGER.error("Caught exception while setting up connectino", e);
        }
    }

    private void getRandom() {
        if (this.sent.isEmpty()) {
            return;
        }
        LOGGER.trace("Getting random key");

        Random rand = new Random();
        int toGetIndex = rand.nextInt(this.sent.size());
        Pair<String> email = this.sent.get(toGetIndex);

        LOGGER.trace("Getting key '{}'", email.key);
        long time1 = System.nanoTime();
        int exitCode = cmd.execute(KVMessage.extractTokens(String.format("get %s", email.key)));
        long time2 = System.nanoTime();

        boolean fail = false;

        if (exitCode != ExitCode.SUCCESS.getValue()) {
            LOGGER.trace("Getting key '{}' was unsuccessful. Removing from sent.", email.key);
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
        LOGGER.trace("Putting random key");

        Random rand = new Random();
        int toSendIndex = rand.nextInt(this.toSend.size());
        Pair<String> email = this.toSend.get(toSendIndex);

        LOGGER.trace("Putting key '{}'", email.key);
        long time1 = System.nanoTime();
        int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s %s", email.key,
                email.value.substring(0, Math.min(email.value.length(), 200)))));
        long time2 = System.nanoTime();

        boolean fail = false;

        if (exitCode == ExitCode.SUCCESS.getValue()) {
            LOGGER.trace("Putting key '{}' was successful. Removing from toSend and adding to sent", email.key);
            toSend.remove(toSendIndex);
            sent.addLast(email);
        } else {
            LOGGER.trace("Putting key '{}' was unsuccessful", email.key);
            fail = true;
        }

        synchronized (this.stats) {
            this.stats.put((time2 - time1) / NANOS_PER_SECOND, fail);
        }
    }

    private void deleteRandom() {
        if (this.sent.size() < 0.3 * this.totalEmailCount) {
            return;
        }
        LOGGER.trace("Deleting random key");

        Random rand = new Random();
        int toDeleteIndex = rand.nextInt(this.sent.size());
        Pair<String> email = this.sent.get(toDeleteIndex);

        LOGGER.trace("Deleting key '{}'", email.key);
        long time1 = System.nanoTime();
        int exitCode = cmd.execute(KVMessage.extractTokens(String.format("put %s null", email.key)));
        long time2 = System.nanoTime();

        boolean fail = false;

        if (exitCode == ExitCode.SUCCESS.getValue()) {
            LOGGER.trace("Deleting key '{}' was successful. Deleting from send and adding to toSend", email.key);
            sent.remove(toDeleteIndex);
            toSend.addLast(email);
        } else {
            LOGGER.trace("Deleting key '{}' was unsuccessful", email.key);
            fail = true;
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
