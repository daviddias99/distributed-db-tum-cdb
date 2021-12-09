package de.tum.i13.shared.persistentstorage;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DistributedPersistentStorageTest {

    @Mock(name = "Persistent storage")
    private NetworkPersistentStorage persistentStorage;

    @Mock(name = "Mock message")
    private KVMessage message;

    @Test
    void retriesGetOnServerStopped() throws PutException, GetException {
        var storage = new DistributedPersistentStorage(persistentStorage);

        when(message.getStatus())
                .thenReturn(KVMessage.StatusType.SERVER_STOPPED);
        when(persistentStorage.get(anyString()))
                .thenReturn(message);

        assertThatExceptionOfType(GetException.class)
                .isThrownBy(() -> storage.get("myKey"))
                .withMessageContaining("maximum number of retries");

        verify(persistentStorage, times(Constants.MAX_REQUEST_RETRIES + 1)).get("myKey");
    }

    @Test
    void retriesPutOnServerStopped() throws PutException {
        var storage = new DistributedPersistentStorage(persistentStorage);

        when(message.getStatus())
                .thenReturn(KVMessage.StatusType.SERVER_STOPPED);
        when(persistentStorage.put(anyString(), anyString()))
                .thenReturn(message);

        assertThatExceptionOfType(PutException.class)
                .isThrownBy(() -> storage.put("myKey", "myValue"))
                .withMessageContaining("maximum number of retries");

        verify(persistentStorage, times(Constants.MAX_REQUEST_RETRIES + 1)).put("myKey", "myValue");
    }

}