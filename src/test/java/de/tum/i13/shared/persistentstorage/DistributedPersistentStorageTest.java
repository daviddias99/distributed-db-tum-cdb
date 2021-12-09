package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.net.CommunicationClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DistributedPersistentStorageTest {

    @Mock(name = "Persistent storage")
    private NetworkPersistentStorage persistentStorage;

    @Mock(name = "Mock message")
    private KVMessage serverMessage;

    @InjectMocks
    @Spy
    private DistributedPersistentStorage distributedStorage;

    @SuppressWarnings("java:S5790")
    abstract class RetryTest {

        @BeforeEach
        abstract void configureServerStatus();

        @Test
        void retriesGet() throws GetException {
            when(persistentStorage.get(anyString()))
                    .thenReturn(serverMessage);

            assertThatExceptionOfType(GetException.class)
                    .isThrownBy(() -> distributedStorage.get("myKey"))
                    .withMessageContaining("maximum number of retries");

            verify(persistentStorage, times(Constants.MAX_REQUEST_RETRIES + 1)).get("myKey");
        }

        @Test
        void retriesPut() throws PutException {
            when(persistentStorage.put(anyString(), anyString()))
                    .thenReturn(serverMessage);

            assertThatExceptionOfType(PutException.class)
                    .isThrownBy(() -> distributedStorage.put("myKey", "myValue"))
                    .withMessageContaining("maximum number of retries");

            verify(persistentStorage, times(Constants.MAX_REQUEST_RETRIES + 1)).put("myKey", "myValue");
        }

    }

    @Nested
    class WhenServerStoppedTest extends RetryTest {

        @BeforeEach
        @Override
        void configureServerStatus() {
            when(serverMessage.getStatus())
                    .thenReturn(KVMessage.StatusType.SERVER_STOPPED);
        }

    }

    @Nested
    class WhenServerWriteLockedTest extends RetryTest {

        @BeforeEach
        @Override
        void configureServerStatus() {
            when(serverMessage.getStatus())
                    .thenReturn(KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

    }

    @Nested
    class WhenServerNotResponsibleTest {

        @Mock(name = "Key Range Message")
        private KVMessage keyRangeMessage;

        @BeforeEach
        void configureMessageResponses() throws CommunicationClientException {
            when(serverMessage.getStatus())
                    .thenReturn(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
                    .thenReturn(KVMessage.StatusType.ERROR);

            when(persistentStorage.sendAndReceive(any(KVMessage.class)))
                    .thenReturn(keyRangeMessage);
            when(keyRangeMessage.getStatus())
                    .thenReturn(KVMessage.StatusType.KEYRANGE_SUCCESS);
            when(keyRangeMessage.getKey())
                    .thenReturn("1,3,location1:42;4,8,location2:31;");
        }

        @Test
        void retriesWithNewMetadataOnGet() throws GetException, CommunicationClientException {
            when(persistentStorage.get(anyString()))
                    .thenReturn(serverMessage);

            final KVMessage message = distributedStorage.get("key");
            assertThat(message.getStatus())
                    .isEqualTo(KVMessage.StatusType.ERROR);

            final InOrder inOrder = Mockito.inOrder(persistentStorage);

            inOrder.verify(persistentStorage)
                    .get("key");
            inOrder.verify(persistentStorage)
                    .sendAndReceive(argThat(
                            argMessage -> argMessage.getStatus() == KVMessage.StatusType.KEYRANGE)
                    );
            inOrder.verify(persistentStorage)
                    .connectAndReceive("location1", 42);
            inOrder.verify(persistentStorage)
                    .get("key");
        }

        @Test
        void retriesWithNewMetadataOnPut() throws CommunicationClientException, PutException {
            when(persistentStorage.put(anyString(), anyString()))
                    .thenReturn(serverMessage);

            final KVMessage message = distributedStorage.put("key", "value");
            assertThat(message.getStatus())
                    .isEqualTo(KVMessage.StatusType.ERROR);

            final InOrder inOrder = Mockito.inOrder(persistentStorage);

            inOrder.verify(persistentStorage)
                    .put("key", "value");
            inOrder.verify(persistentStorage)
                    .sendAndReceive(argThat(
                            argMessage -> argMessage.getStatus() == KVMessage.StatusType.KEYRANGE)
                    );
            inOrder.verify(persistentStorage)
                    .connectAndReceive("location1", 42);
            inOrder.verify(persistentStorage)
                    .put("key", "value");
        }

    }


}