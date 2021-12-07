package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.net.CommunicationClientException;
import de.tum.i13.shared.net.NetworkMessageServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DistributedPersistentStorageTest {

    @Mock(name = "Message Server")
    private NetworkMessageServer messageServer;

    @Mock(name = "Mock message")
    private KVMessage message;

    @Test
    void retriesOnServerStopped() throws CommunicationClientException {
        var storage = new DistributedPersistentStorage(messageServer);

        when(messageServer.receive())
                .thenReturn("SERVER_STOPPED");

        assertThatExceptionOfType(CommunicationClientException.class)
                .isThrownBy(() -> storage.sendAndReceive(message))
                .withMessageContaining("maximum number of retries");
    }

}