package de.tum.i13;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.state.ServerState;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

class TestKVCommandProcessor {

    @Test
    void correctParsingOfPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.UNDEFINED));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState(ServerState.State.ACTIVE));
        kvcp.process("put key hello", PeerType.CLIENT);

        verify(kv).put("key", "hello");
    }

    @Test
    void commandNotExecutedWhenStopped() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.UNDEFINED));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState());
        String response = kvcp.process("put key hello", PeerType.CLIENT);
        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.SERVER_STOPPED);
        ;

        verify(kv, never()).put("key", "hello");
    }

    @Test
    void ecsBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.UNDEFINED));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState());
        kvcp.process("put key hello", PeerType.ECS);
        verify(kv).put("key", "hello");
    }

    @Test
    void serverBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.UNDEFINED));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState());
        kvcp.process("put key hello", PeerType.SERVER);
        verify(kv).put("key", "hello");
    }
}
