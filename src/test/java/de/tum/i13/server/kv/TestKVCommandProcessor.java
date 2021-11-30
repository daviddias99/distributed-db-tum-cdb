package de.tum.i13.server.kv;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.server.state.ServerState.State;

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
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState(ServerState.State.ACTIVE));
        kvcp.process("put key hello", PeerType.CLIENT);

        verify(kv).put("key", "hello");
    }

    @Test
    void commandNotExecutedWhenStopped() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
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

        verify(kv, never()).put("key", "hello");
    }

    @Test
    void ecsBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState());
        kvcp.process("put key hello", PeerType.ECS);
        verify(kv).put("key", "hello");
    }

    @Test
    void serverBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, new ServerState());
        kvcp.process("put key hello", PeerType.SERVER);
        verify(kv).put("key", "hello");
    }

    @Test
    void ecsWriteLockResponse() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        ServerState serverState = new ServerState();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, serverState);
        String response = kvcp.process("ecs_write_lock", PeerType.ECS);
        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.SERVER_WRITE_LOCK);

        assertThat(serverState.canWrite()).isFalse();
    }

    @Test
    void clientCantWriteLock() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        ServerState serverState = new ServerState(State.ACTIVE);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, serverState);
        String response = kvcp.process("ecs_write_lock", PeerType.CLIENT);
        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.ERROR);

        assertThat(serverState.canWrite()).isTrue();
    }

    @Test
    void clientReceivesWriteLockOnPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        ServerState serverState = new ServerState(State.ACTIVE);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, serverState);
        kvcp.process("ecs_write_lock", PeerType.ECS);
        String response = kvcp.process("put key value", PeerType.CLIENT);
        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.SERVER_WRITE_LOCK);

        verify(kv, never()).put("key", "value");
    }

    @Test
    void clientReceivesWriteLockOnDelete() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        ServerState serverState = new ServerState(State.ACTIVE);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, serverState);
        kvcp.process("ecs_write_lock", PeerType.ECS);
        String response = kvcp.process("delete key", PeerType.CLIENT);
        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.SERVER_WRITE_LOCK);

        verify(kv, never()).put("key", null);
    }

    @Test
    void getSucceedsOnWriteLock() throws GetException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.get("key")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        ServerState serverState = new ServerState(State.ACTIVE);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, serverState);
        kvcp.process("ecs_write_lock", PeerType.ECS);
        kvcp.process("get key", PeerType.CLIENT);
        assertThat(serverState.canWrite()).isFalse();
        verify(kv).get("key");
    }
}
