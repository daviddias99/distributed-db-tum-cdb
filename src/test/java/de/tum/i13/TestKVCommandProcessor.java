package de.tum.i13;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PersistentStorage;
import de.tum.i13.server.kv.PutException;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.server.state.ServerState.State;
import de.tum.i13.shared.NetworkLocationImpl;
import de.tum.i13.shared.hashing.ConsistentHashRing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

class TestKVCommandProcessor {

    @Test
    void correctParsingOfPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        ServerState state =  new ServerState(State.ACTIVE, new NetworkLocationImpl("127.0.0.1", 25565));
        ConsistentHashRing hr = mock(ConsistentHashRing.class);
        when(hr.getResponsibleNetworkLocation("key")).thenReturn(Optional.of(new NetworkLocationImpl("127.0.0.1", 25565)));
        state.setRingMetadata(hr);
        
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state);
        kvcp.process("put key hello", PeerType.CLIENT);

        verify(kv).put("key", "hello");
    }
}
