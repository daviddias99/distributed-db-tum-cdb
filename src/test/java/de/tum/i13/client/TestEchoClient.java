package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.client.exceptions.ClientExceptionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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
    void correctLocalhost() {
        EchoClient client = assertDoesNotThrow(() -> new EchoClient("localhost", serverSocket.getLocalPort()));
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void defaultConstructor() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void wrongLocalhost() {
        EchoClient client = new EchoClient();
        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> new EchoClient("localhost00", serverSocket.getLocalPort()))
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.UNKNOWN_HOST);
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void connectSingleTime() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void connectMultipleTimes() {
        EchoClient client = new EchoClient();

        for (int i = 0; i < 3; i++) {
            assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                    .doesNotThrowAnyException();
            assertThat(client.isConnected()).isTrue();
        }
    }

    @Test
    void connectIncorrectly() {
        EchoClient client = new EchoClient();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.connect("localhost00", serverSocket.getLocalPort()))
                .isInstanceOf(ClientException.class)
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.UNKNOWN_HOST);
    }

    @Test
    void connectAndReceiveSimultaneously() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        byte[] receivedData = assertDoesNotThrow(() -> client.connectAndReceive("localhost", serverSocket.getLocalPort()));
        assertThat(receivedData)
                .asString()
                .isEqualTo("Welcome!");
    }

    @Test
    void connectAndReceiveSeparately() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        byte[] receivedData = assertDoesNotThrow(client::receive);
        assertThat(receivedData)
                .asString()
                .isEqualTo("Welcome!");
    }

    @Test
    void unconnectedReceive() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(client::receive)
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.UNCONNECTED);
    }

    @Test
    void connectAndSend() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();

        assertThatCode(() -> client.send("Hello!".getBytes()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void unconnectedSend() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.send("test".getBytes()))
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.UNCONNECTED);
    }

    @Test
    void messageToLarge() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.send(new byte[1024 * 256]))
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.MESSAGE_TOO_LARGE);
    }

    @Test
    void disconnect() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();
        assertThatCode(client::disconnect)
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void disconnectUnconnected() {
        EchoClient client = new EchoClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(client::disconnect)
                .extracting(ClientException::getType)
                .isEqualTo(ClientExceptionType.UNCONNECTED);
    }
}
