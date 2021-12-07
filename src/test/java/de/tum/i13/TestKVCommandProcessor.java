package de.tum.i13;

import de.tum.i13.shared.kv.KVCommandProcessor;
import de.tum.i13.shared.kv.KVMessage;
import de.tum.i13.shared.kv.KVMessageImpl;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestKVCommandProcessor {

    @Test
    void correctParsingOfPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process("put key hello");

        verify(kv).put("key", "hello");
    }
}
