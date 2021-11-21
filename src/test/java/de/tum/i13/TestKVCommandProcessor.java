package de.tum.i13;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestKVCommandProcessor {

    @Test
    public void correctParsingOfPut() throws Exception {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.UNDEFINED));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process("put key hello");

        verify(kv).put("key", "hello");
    }
}
