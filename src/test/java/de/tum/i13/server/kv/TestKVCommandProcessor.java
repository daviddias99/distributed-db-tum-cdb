package de.tum.i13.server.kv;

import de.tum.i13.server.kv.commandprocessing.KVCommandProcessor;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.hashing.HashingAlgorithm;
import de.tum.i13.shared.hashing.PrecedingResponsibilityHashRing;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.net.NetworkLocationImpl;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class TestKVCommandProcessor {

    NetworkLocation server1Location = new NetworkLocationImpl("192.168.1.0", 25565);
    ConsistentHashRing ring;
    ECSServerState state;

    @Mock
    HashingAlgorithm hashingAlgorithm;

    @BeforeEach
    void setupData() {
        final NavigableMap<BigInteger, NetworkLocation> networkLocationMap = new TreeMap<>();
        ring = mock(PrecedingResponsibilityHashRing.class,
                withSettings()
                        .useConstructor(hashingAlgorithm, networkLocationMap)
                        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
        );
        NetworkLocation server2Location = new NetworkLocationImpl("192.168.1.1", 25566);
        when(hashingAlgorithm.hash(server2Location))
                .thenReturn(new BigInteger("00000000000000000000000000000000", 16));
        when(hashingAlgorithm.hash(server1Location))
                .thenReturn(new BigInteger("80000000000000000000000000000000", 16));

        ring.addNetworkLocation(server2Location);
        ring.addNetworkLocation(server1Location);
        lenient().doReturn(true).when(ring)
                .isWriteResponsible(any(NetworkLocation.class), anyString());

        state = new ECSServerState(server1Location, new NetworkLocationImpl("127.0.0.1", 25566));
        state.setRingMetadata(ring);
    }

    @Test
    void correctParsingOfPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("put key hello");

        verify(kv).put("key", "hello");
    }

    @Test
    void commandNotExecutedWhenStopped() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        lenient().when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("put key hello");
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
    void respondsToHeartbeatFromEcs() {

        PersistentStorage kv = mock(PersistentStorage.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("ecs_heart_beat");

        assertThat(KVMessage
                .unpackMessage(response))
                        .extracting(
                                KVMessage::getKey,
                                KVMessage::getValue,
                                KVMessage::getStatus)
                        .containsExactly(
                                null,
                                null,
                                KVMessage.StatusType.SERVER_HEART_BEAT);
    }

    @Test
    void serverBypassesStop() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        when(kv.put("key", "hello")).thenReturn(new KVMessageImpl(KVMessage.StatusType.ERROR));
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("put_server key hello");
        verify(kv).put("key", "hello");
    }

    @Test
    void ecsWriteLockResponse() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("ecs_write_lock");
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
    void clientReceivesWriteLockOnPut() throws PutException {

        PersistentStorage kv = mock(PersistentStorage.class);
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        kvcp.process("ecs_write_lock");
        String response = kvcp.process("put key value");
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
        kvcp.process("ecs_write_lock");
        String response = kvcp.process("delete key");
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
        kvcp.process("ecs_write_lock");
        kvcp.process("get key");
        assertThat(state.canWrite()).isFalse();
        verify(kv).get("key");
    }

    @Test
    void getsKeyRange() {
        when(hashingAlgorithm.getMax())
                .thenReturn(Constants.MD5_HASH_MAX_VALUE);


        PersistentStorage kv = mock(PersistentStorage.class);
        state.start();
        KVCommandProcessor kvcp = new KVCommandProcessor(kv, state, new ServerCommunicator(null));
        String response = kvcp.process("keyrange");
        assertThat(KVMessage
        .unpackMessage(response))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        state.getRingMetadata().packWriteRanges(),
                        null,
                        KVMessage.StatusType.KEYRANGE_SUCCESS);
        assertThat(state.getRingMetadata().packWriteRanges()).isEqualTo("80000000000000000000000000000001,0,192.168.1.1:25566;1,80000000000000000000000000000000,192.168.1.0:25565;");
    }
}
