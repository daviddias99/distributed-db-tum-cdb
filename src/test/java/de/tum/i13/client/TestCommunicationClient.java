package de.tum.i13.client;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClient;
import de.tum.i13.shared.net.CommunicationClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;

class TestCommunicationClient {

    Thread serverThread;
    ServerSocket serverSocket;
    ServerStub server;

    @BeforeEach
    void createServer() throws IOException {
        serverSocket = new ServerSocket(0);
        server = new ServerStub(serverSocket);
        serverThread = new Thread(server);
        serverThread.start();
    }

    @AfterEach
    void closeServer() throws IOException {
        serverThread.interrupt();
        serverSocket.close();
    }

    @Nested
    class CreationTest {

        @Test
        void createsOnCorrectLocalhost() throws CommunicationClientException {
            try (CommunicationClient client = new CommunicationClient("localhost", serverSocket.getLocalPort())) {
                assertThat(client.isConnected())
                        .isTrue();
            }
        }

        @Test
        void createsOnDefaultConstructor() {
            try (CommunicationClient client = new CommunicationClient()) {
                assertThat(client.isConnected())
                        .isFalse();
            } catch (CommunicationClientException e) {
            }
        }

        @Test
        void doesNotCreateOnWrongAddress() {
            try (CommunicationClient client = new CommunicationClient()) {
                assertThatExceptionOfType(CommunicationClientException.class)
                        .isThrownBy(() -> new CommunicationClient("localhost00", serverSocket.getLocalPort()))
                        .extracting(CommunicationClientException::getType)
                        .isEqualTo(CommunicationClientException.Type.UNKNOWN_HOST);
                assertThat(client.isConnected())
                        .isFalse();
            } catch (CommunicationClientException e) {
            }
                    
        }

    }

    @Nested
    class CreatedClientTest {

        CommunicationClient client;

        @BeforeEach
        void createsClient() {
            client = new CommunicationClient();
            assumeThat(client.isConnected())
                    .withFailMessage("Client was unexpectedly connected after creation")
                    .isFalse();
        }

        @Test
        void connectsSingleTime() throws CommunicationClientException {
            client.connect("localhost", serverSocket.getLocalPort());
            assertThat(client.isConnected())
                    .isTrue();
        }

        @Test
        void connectsMultipleTimes() throws CommunicationClientException {
            for (int i = 0; i < 3; i++) {
                client.connect("localhost", serverSocket.getLocalPort());
                assertThat(client.isConnected())
                        .isTrue();
            }
        }

        @Test
        void doesNotConnectOnWrongAddress() {
            assertThatExceptionOfType(CommunicationClientException.class)
                    .isThrownBy(() -> client.connect("localhost00", serverSocket.getLocalPort()))
                    .extracting(CommunicationClientException::getType)
                    .isEqualTo(CommunicationClientException.Type.UNKNOWN_HOST);
        }

        @Test
        void connectsAndReceivesSimultaneously() throws CommunicationClientException {
            assertThat(client.connectAndReceive("localhost", serverSocket.getLocalPort()))
                    .isEqualTo("Welcome!");
        }

        @Test
        void doesNotReceiveUnconnected() {
            assertThatExceptionOfType(CommunicationClientException.class)
                    .isThrownBy(client::receive)
                    .extracting(CommunicationClientException::getType)
                    .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
        }

        @Test
        void connectSendAndReceive() throws CommunicationClientException {
            client.connectAndReceive("localhost", serverSocket.getLocalPort());

            client.send("Requesting answer");
            assertThat(client.receive())
                    .isEqualTo("Answer!");
        }

        @Test
        void doesNotSendUnconnected() {
            assertThatExceptionOfType(CommunicationClientException.class)
                    .isThrownBy(() -> client.send("test"))
                    .extracting(CommunicationClientException::getType)
                    .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
        }

        @Test
        void doesNotDisconnectUnconnected() {
            assertThatExceptionOfType(CommunicationClientException.class)
                    .isThrownBy(client::disconnect)
                    .extracting(CommunicationClientException::getType)
                    .isEqualTo(CommunicationClientException.Type.UNCONNECTED);
        }

        @Nested
        class ConnectedClientTest {

            @BeforeEach
            void connectsClient() throws CommunicationClientException {
                client.connect("localhost", serverSocket.getLocalPort());
                assumeThat(client.isConnected())
                        .withFailMessage("Client could not connect to local server")
                        .isTrue();
            }

            @Test
            void receivesWelcomeMessage() throws CommunicationClientException {
                assertThat(client.receive())
                        .isEqualTo("Welcome!");
            }

            @Test
            void sendsMessage() {
                assertThatCode(() -> client.send("Hello"))
                        .doesNotThrowAnyException();
            }

            @Test
            void doesNotSendTooLargeMessage() {
                String veryLongString = " ".repeat(Constants.BYTES_PER_KB * (Constants.MAX_MESSAGE_SIZE_KB + 1));

                assumeThat(veryLongString.getBytes(Constants.TELNET_ENCODING))
                        .withFailMessage("Message to send did not exceed size limit")
                        .hasSizeGreaterThan(Constants.MAX_MESSAGE_SIZE_BYTES);

                assertThatExceptionOfType(CommunicationClientException.class)
                        .isThrownBy(() -> client.send(veryLongString))
                        .extracting(CommunicationClientException::getType)
                        .isEqualTo(CommunicationClientException.Type.MESSAGE_TOO_LARGE);
            }

            @Test
            void disconnects() throws CommunicationClientException {
                client.disconnect();
                assertThat(client.isConnected())
                        .isFalse();
            }

            @Test
            void doesNotDisconnectOnConnectingToInvalidServer() {
                assertThatExceptionOfType(CommunicationClientException.class)
                        .isThrownBy(() -> client.connect("totallyWrongAddress", 123));
                assertThat(client.isConnected())
                        .isTrue();
            }

        }

    }

}
