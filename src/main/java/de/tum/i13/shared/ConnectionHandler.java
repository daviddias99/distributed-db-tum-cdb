package de.tum.i13.shared;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * TODO: This interface should either change name or be merged with a more
 * appropriate one. Changed this here because it seemed that this shouldn't be a
 * responsability for a command processor
 */
public interface ConnectionHandler {
  String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);

  void connectionClosed(InetAddress address);
}
