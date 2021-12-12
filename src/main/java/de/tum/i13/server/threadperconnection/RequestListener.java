package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVConnectionHandler;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.ConnectionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RequestListener implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(RequestListener.class);

  private CommandProcessor<String> commandProcessor;
  private String listenAddress;
  private int listenPort;

  public RequestListener(String listenAddress, int listenPort, CommandProcessor<String> commandProcessor) {
    this.listenAddress = listenAddress;
    this.listenPort = listenPort;
    this.commandProcessor = commandProcessor;
  }

  @Override
  public void run() {

    try (final ServerSocket serverSocket = new ServerSocket()) {

      // Hook not used to allow receiving messages after shutdown (for handoff)
      // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      //   LOGGER.info("Closing server socket");
      //   try {
      //     serverSocket.close();
      //   } catch (IOException ex) {
      //     LOGGER.fatal("Caught exception, while closing server socket", ex);
      //   }
      // }));

      // bind to localhost only
      serverSocket.bind(new InetSocketAddress(this.listenAddress, this.listenPort));


      LOGGER.info("Listening for requests at {}", serverSocket);

      ConnectionHandler cHandler = new KVConnectionHandler();

      // Use ThreadPool
      ExecutorService executorService = Executors.newCachedThreadPool();

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

    } catch (IOException ex) {
      LOGGER.fatal("Caught exception, while creating and binding server socket", ex);
    }
  }
}
