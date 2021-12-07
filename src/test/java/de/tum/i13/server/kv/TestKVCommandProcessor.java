package de.tum.i13.server.kv;

import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.TreeMapServerMetadata;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import static org.mockito.Mockito.never;

class TestKVCommandProcessor {

    static NetworkLocation server1Location = new NetworkLocationImpl("192.168.1.0", 25565);
    static ConsistentHashRing ring;
    static ServerState state;

    @BeforeAll
    static void createRing() {
        NetworkLocation server2Location = new NetworkLocationImpl("192.168.1.1", 25566);

        ring = new TreeMapServerMetadata();
        ring.addNetworkLocation(new BigInteger("00000000000000000000000000000000", 16), server2Location);
        ring.addNetworkLocation(new BigInteger("80000000000000000000000000000000", 16), server1Location);
    }

    @BeforeEach
    void setState() {
        state = new ServerState(server1Location, new NetworkLocationImpl("127.0.0.1", 25566));
        state.setRingMetadata(ring);
    }

    @Test
    void correctParsingOfPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("put key hello", PeerType.CLIENT);

        verify(kv).put("key", "hello");
    }

    @Test
    void commandNotExecutedWhenStopped() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
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
    void respondstoHeatbeatFromEcs() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("heart_beat", PeerType.ECS);

        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.HEART_BEAT);
    }

    @Test
    void serverBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("put key hello", PeerType.SERVER);
        verify(kv).put("key", "hello");
    }

    @Test
    void ecsWriteLockResponse() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
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

        assertThat(state.canWrite()).isFalse();
    }

    @Test
    void clientCantWriteLock() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
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

        assertThat(state.canWrite()).isTrue();
    }

    @Test
    void clientReceivesWriteLockOnPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
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
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
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
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("ecs_write_lock", PeerType.ECS);
        kvcp.process("get key", PeerType.CLIENT);
        assertThat(state.canWrite()).isFalse();
        verify(kv).get("key");
    }

    @Test
    void getsKeyRange() throws GetException {

        PersistentStorage kv = mock(PersistentStorage.class);
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("keyrange", PeerType.CLIENT);
        assertThat(KVMessage
        .unpackMessage(response))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        state.getRingMetadata().packMessage(),
                        null,
                        KVMessage.StatusType.KEYRANGE_SUCCESS);
        assertThat(state.getRingMetadata().packMessage()).isEqualTo("80000000000000000000000000000001,0,192.168.1.1:25566;1,80000000000000000000000000000000,192.168.1.0:25565;");
    }
}
