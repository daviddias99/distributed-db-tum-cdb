package de.tum.i13.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.CommunicationClient;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestCommunicationClient {

    private static Thread serverThread;
    private static ServerSocket serverSocket;
    private static ServerStub server;

    @BeforeEach
    public void createServer() {
        try {
            TestCommunicationClient.serverSocket = new ServerSocket(0);
            TestCommunicationClient.server = new ServerStub(serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TestCommunicationClient.serverThread = new Thread(server);
        TestCommunicationClient.serverThread.start();
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
        CommunicationClient client = assertDoesNotThrow(() -> new CommunicationClient("localhost", serverSocket.getLocalPort()));
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void defaultConstructor() {
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void wrongLocalhost() {
        CommunicationClient client = new CommunicationClient();
        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(() -> new CommunicationClient("localhost00", serverSocket.getLocalPort()))
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.UNKNOWN_HOST);
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void connectSingleTime() {
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void connectMultipleTimes() {
        CommunicationClient client = new CommunicationClient();

        for (int i = 0; i < 3; i++) {
            assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                    .doesNotThrowAnyException();
            assertThat(client.isConnected()).isTrue();
        }
    }

    @Test
    void connectIncorrectly() {
        CommunicationClient client = new CommunicationClient();

        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(() -> client.connect("localhost00", serverSocket.getLocalPort()))
                .isInstanceOf(CommunicationClientException.class)
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.UNKNOWN_HOST);
    }

    @Test
    void connectAndReceiveSimultaneously() {
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        byte[] receivedData = assertDoesNotThrow(() -> client.connectAndReceive("localhost", serverSocket.getLocalPort()));
        assertThat(receivedData)
                .asString()
                .isEqualTo("Welcome!");
    }

    @Test
    void connectAndReceiveSeparately() {
        CommunicationClient client = new CommunicationClient();
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
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(client::receive)
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
    }

    @Test
    void connectAndSend() {
        CommunicationClient client = new CommunicationClient();
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
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(() -> client.send("test".getBytes()))
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
    }

    @Test
    void messageToLarge() {
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(() -> client.send(new byte[1024 * 256]))
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.MESSAGE_TOO_LARGE);
    }

    @Test
    void disconnect() {
        CommunicationClient client = new CommunicationClient();
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
        CommunicationClient client = new CommunicationClient();
        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(client::disconnect)
                .extracting(CommunicationClientException::getType)
                .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
    }
}
