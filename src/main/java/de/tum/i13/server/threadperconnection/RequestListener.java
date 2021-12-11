package de.tum.i13.server.threadperconnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.jmx.Server;

import de.tum.i13.ecs.ECSCommandProcessor;
import de.tum.i13.server.kv.KVConnectionHandler;
import de.tum.i13.server.kv.commandprocessing.KVEcsCommandProcessor;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;

public class RequestListener implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(RequestListener.class);

  private CommandProcessor<String> commandProcessor;
  private String listenAddress;
  private int listenPort;
  private ServerCommunicator ecsCommunicator;
  private KVEcsCommandProcessor ecsCommandProcessor;

  public RequestListener(String listenAddress, int listenPort, CommandProcessor<String> commandProcessor, ServerCommunicator ecsCommunicator, KVEcsCommandProcessor ecsCommandProcessor) {
    this.listenAddress = listenAddress;
    this.listenPort = listenPort;
    this.commandProcessor = commandProcessor;
    this.ecsCommunicator = ecsCommunicator;
    this.ecsCommandProcessor = ecsCommandProcessor;
  }

  @Override
  public void run() {

    try (final ServerSocket serverSocket = new ServerSocket()) {

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOGGER.info("Closing server socket");
        try {
          serverSocket.close();
        } catch (IOException ex) {
          LOGGER.fatal("Caught exception, while closing server socket", ex);
        }
      }));

      // bind to localhost only
      serverSocket.bind(new InetSocketAddress(this.listenAddress, this.listenPort));
      // Request metadata from ECS
      LOGGER.info("Requesting metadata do ECS");
      ecsCommandProcessor.process(ecsCommunicator.signalStart(listenAddress, Integer.toString(listenPort)));

      LOGGER.info("Listening for requests at {}", serverSocket);

      ConnectionHandler cHandler = new KVConnectionHandler();

      // Use ThreadPool
      ExecutorService executorService = Executors.newFixedThreadPool(Constants.CORE_POOL_SIZE);

      try {
        while (!Thread.interrupted()) {
          // accept a connection
          Socket clientSocket = serverSocket.accept();
          LOGGER.info("New connection at {}", clientSocket);

          // start a new Thread for this connection
          executorService.submit(new ConnectionHandleThread(commandProcessor, cHandler, clientSocket,
              (InetSocketAddress) serverSocket.getLocalSocketAddress()));
        }
      } catch (IOException ex) {
        LOGGER.fatal("Caught exception while accepting client request", ex);
        LOGGER.info("Closing executor service");
        executorService.shutdown();
      }

    } catch (IOException | CommunicationClientException ex) {
      LOGGER.fatal("Caught exception, while creating and binding server socket", ex);
    }
  }
}