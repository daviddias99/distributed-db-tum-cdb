package de.tum.i13.client;

import org.junit.jupiter.api.*;

import de.tum.i13.client.exceptions.ClientException.ClientException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;

public class TestEchoClient {

  private static Thread serverThread;
  private static ServerSocket serverSocket;
  private static ServerStub server;

  @BeforeEach
  public void createServer() {
    try {
      TestEchoClient.serverSocket = new ServerSocket(0);
      TestEchoClient.server = new ServerStub(serverSocket);
    } catch (IOException e) {
      e.printStackTrace();
    }
    TestEchoClient.serverThread = new Thread(server);
    TestEchoClient.serverThread.start();
  }

  @AfterEach
  public void closeServer() {
    serverThread.interrupt();
    try {
      serverSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @BeforeEach // gets called before the class initializes, perfect place to connect to a
              // server
  public void initialize() {
  }

  @AfterEach // tearing down, perfect place to properly disconnect from a server or close a
             // socket
  public void teardown() {

  }

  @Test
  public void testConstructor1() {

    try {
      EchoClient client = new EchoClient("localhost", serverSocket.getLocalPort());
      assertEquals(true, client.isConnected());
    } catch (ClientException e) {
      System.out.println(e);
      fail();
    }
  }

  @Test
  public void testConstructor2() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());
  }

  @Test
  public void testConstructor3() {
    EchoClient client = new EchoClient();
    try {
      client = new EchoClient("localhost00", serverSocket.getLocalPort());
    } catch (ClientException e) {
      assertEquals(false, client.isConnected());
      return;
    }
  }

  @Test
  public void testConnectMethod() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.connect("localhost", serverSocket.getLocalPort());
      assertEquals(true, client.isConnected());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testConnectMethod2() {
    EchoClient client = new EchoClient();

    try {
      client.connect("localhost", serverSocket.getLocalPort());
      assertEquals(true, client.isConnected());

      client.connect("localhost", serverSocket.getLocalPort());
      assertEquals(true, client.isConnected());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testConnectMethod3() {
    EchoClient client = new EchoClient();

    try {
      client.connect("localhost00", serverSocket.getLocalPort());
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testConnectAndReceive() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      byte[] receivedData = client.connectAndReceive("localhost", serverSocket.getLocalPort());
      String receivedString = new String(receivedData);
      assertEquals("Welcome!", receivedString);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testReceive1() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.connect("localhost", serverSocket.getLocalPort());
      byte[] receivedData = client.receive();
      String receivedString = new String(receivedData);
      assertEquals("Welcome!", receivedString);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testReceive2() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.receive();
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testSend2() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.send("test".getBytes());
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testSend3() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.connect("localhost", serverSocket.getLocalPort());
      client.send(new byte[1024 * 256]);
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testDisconnect1() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.connect("localhost", serverSocket.getLocalPort());
      assertEquals(true, client.isConnected());
      client.disconnect();
      assertEquals(false, client.isConnected());

    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testDisconnect2() {
    EchoClient client = new EchoClient();
    assertEquals(false, client.isConnected());

    try {
      client.disconnect();
    } catch (Exception e) {
      return;
    }
    fail();
  }
}
