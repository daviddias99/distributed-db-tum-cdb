package de.tum.i13.server.kv.commandprocessing.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.Config;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.commandprocessing.KVEcsCommandProcessor;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.net.CommunicationClientException;

/**
 * Handler that manages server shutdown (handoff).
 */
public class ShutdownHandler implements Runnable {

  private ServerCommunicator ecsComms;
  private KVEcsCommandProcessor processor;
  private Config config;
  private Thread listeningThread;
  private ServerState state;

  /**
   * Create a new shutdown handler
   * 
   * @param ecsComms  ECS communications interface
   * @param processor processor of commands from the ECS
   */
  public ShutdownHandler(ServerCommunicator ecsComms, KVEcsCommandProcessor processor, Config config,
      Thread listeningThread, ServerState state) {
    this.ecsComms = ecsComms;
    this.processor = processor;
    this.config = config;
    this.listeningThread = listeningThread;
    this.state = state;
  }

  @Override
  public void run() {
    Logger LOGGER = LogManager.getLogger(ShutdownHandler.class);
    LOGGER.info("Starting server shutdown procedure");

    // Check if comms are connected
    if (!ecsComms.isConnected()) {
      try {
        LOGGER.info("ECS not connected, trying to reconnect");
        ecsComms.reconnect();
      } catch (CommunicationClientException ex) {
        LOGGER.fatal("Could not connect to ECS for shutdown", ex);
        return;
      }
    }

    try {
      this.state.shutdown();
      KVMessage ecsResponse = ecsComms.sendShutdown(config.listenAddress, config.port);
      KVMessage message;

      /**
       * Expected message sequence (< outgoing, > inbound)
       * NOTE: Currently these embedded processing cycle isn't being used because the
       * resposes are sent to the listening address. Because of this, an interrupt of the server thread is needed
       * < SERVER_SHUTDOWN
       * > ECS_WRITE_LOCK
       * < SERVER_WRITE_LOCK
       * > ECS_HANDOFF
       * < SERVER_HANDOFF_SUCCESS (sent by process created by EcsCommandProcessor)
       */
      do {
        message = processor.process(ecsResponse);

        // null message means that there is no response to be sent
        if (message == null) {
          break;
        }

        ecsResponse = ecsComms.sendAndReceive(message);
      } while (true);

      LOGGER.info("Finished shutdown procedure");
    } catch (CommunicationClientException e) {
      LOGGER.fatal("Error while communicating with ECS for shutdown", e);
    }

    // Stop listenint thread
    listeningThread.interrupt();
  }
}
