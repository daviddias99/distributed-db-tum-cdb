package de.tum.i13.client;

import org.junit.jupiter.api.*;

import de.tum.i13.client.exceptions.ClientException.ClientException;
import de.tum.i13.client.exceptions.ClientException.ClientExceptionType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;

class TestEchoClient {

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

    @Test
    void testConstructor1() {

        try {
            EchoClient client = new EchoClient("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());
        } catch (ClientException e) {
            System.out.println(e);
            fail();
        }
    }

    @Test
    void testConstructor2() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());
    }

    @Test
    void testConstructor3() {
        EchoClient client = new EchoClient();
        try {
            client = new EchoClient("localhost00", serverSocket.getLocalPort());
        } catch (ClientException e) {
            assertEquals(false, client.isConnected());
            assertEquals(ClientExceptionType.UNKNOWN_HOST, e.getType());
            return;
        }
    }

    @Test
    void testConnectMethod() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());
        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testConnectMethod2() {
        EchoClient client = new EchoClient();

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());

            client.connect("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());
        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testConnectMethod3() {
        EchoClient client = new EchoClient();

        try {
            client.connect("localhost00", serverSocket.getLocalPort());
        } catch (ClientException e) {
            assertEquals(ClientExceptionType.UNKNOWN_HOST, e.getType());
            return;
        }
        fail();
    }

    @Test
    void testConnectAndReceive() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            byte[] receivedData = client.connectAndReceive("localhost", serverSocket.getLocalPort());
            String receivedString = new String(receivedData);
            assertEquals("Welcome!", receivedString);
        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testReceive1() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            byte[] receivedData = client.receive();
            String receivedString = new String(receivedData);
            assertEquals("Welcome!", receivedString);
        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testReceive2() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.receive();
        } catch (ClientException e) {
            assertEquals(ClientExceptionType.UNCONNECTED, e.getType());
            return;
        }
        fail();
    }

    @Test
    void testSend1() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());
            client.send("Hello!".getBytes());
            assertEquals(true, client.isConnected());
        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testSend2() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.send("test".getBytes());
        } catch (ClientException e) {
            assertEquals(ClientExceptionType.UNCONNECTED, e.getType());
            return;
        }
        fail();
    }

    @Test
    void testSend3() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            client.send(new byte[1024 * 256]);
        } catch (ClientException e) {
            assertEquals(ClientExceptionType.MESSAGE_TOO_LARGE, e.getType());
            return;
        }
        fail();
    }

    @Test
    void testDisconnect1() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.connect("localhost", serverSocket.getLocalPort());
            assertEquals(true, client.isConnected());
            client.disconnect();
            assertEquals(false, client.isConnected());

        } catch (ClientException e) {
            fail();
        }
    }

    @Test
    void testDisconnect2() {
        EchoClient client = new EchoClient();
        assertEquals(false, client.isConnected());

        try {
            client.disconnect();
        } catch (ClientException e) {
            assertEquals(ClientExceptionType.UNCONNECTED, e.getType());
            return;
        }
        fail();
    }
}
