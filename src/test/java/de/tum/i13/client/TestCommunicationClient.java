package de.tum.i13.client;

import de.tum.i13.client.net.ClientException;
import de.tum.i13.client.net.CommunicationClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    private static CommunicationClient client;

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

        TestCommunicationClient.client = new CommunicationClient();
    }

    @AfterEach
    public void closeServer() {
        serverThread.interrupt();

        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            client.close();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    @Test
    void correctLocalhost() {
        CommunicationClient client = assertDoesNotThrow(
                () -> new CommunicationClient("localhost", serverSocket.getLocalPort()));
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void defaultConstructor() {
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void wrongLocalhost() {

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> new CommunicationClient("localhost00", serverSocket.getLocalPort()))
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.UNKNOWN_HOST);
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    void connectSingleTime() {

        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void connectMultipleTimes() {
        for (int i = 0; i < 3; i++) {
            assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                    .doesNotThrowAnyException();
            assertThat(client.isConnected()).isTrue();
        }
    }

    @Test
    void connectIncorrectly() {

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.connect("localhost00", serverSocket.getLocalPort()))
                .isInstanceOf(ClientException.class)
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.UNKNOWN_HOST);
    }

    @Test
    void connectAndReceiveSimultaneously() {

        assertThat(client.isConnected()).isFalse();

        byte[] receivedData = assertDoesNotThrow(
                () -> client.connectAndReceive("localhost", serverSocket.getLocalPort()));
        assertThat(receivedData)
                .asString()
                .isEqualTo("Welcome!");
    }

    @Test
    void connectAndReceiveSeparately() {

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

        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(client::receive)
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.UNCONNECTED);
    }

    @Test
    void connectAndSend() {

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

        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.send("test".getBytes()))
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.UNCONNECTED);
    }

    @Test
    void messageToLarge() {

        assertThat(client.isConnected()).isFalse();

        assertThatCode(() -> client.connect("localhost", serverSocket.getLocalPort()))
                .doesNotThrowAnyException();
        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(() -> client.send(new byte[1024 * 256]))
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.MESSAGE_TOO_LARGE);
    }

    @Test
    void disconnect() {

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

        assertThat(client.isConnected()).isFalse();

        assertThatExceptionOfType(ClientException.class)
                .isThrownBy(client::disconnect)
                .extracting(ClientException::getType)
                .isEqualTo(ClientException.Type.UNCONNECTED);
    }
}
