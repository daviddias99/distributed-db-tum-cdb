package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

abstract class CacheTest {

    abstract Cache getCache(int size);

    @Test
    void displacesKeyWhenFull() {
        final Cache cache = getCache(3);

        for (int i = 0; i < 4; i++) {
            cache.put("key" + i, "value" + i);
        }

        final Set<KVMessage> responseMessages = IntStream.range(0, 3).boxed()
                .map(i -> "key" + i)
                .map(cache::get)
                .collect(Collectors.toSet());

        assertThat(responseMessages)
                .filteredOn(KVMessage::getStatus, KVMessage.StatusType.GET_ERROR)
                .singleElement()
                .satisfiesAnyOf(
                        kvMessage -> assertThat(kvMessage.getKey()).isEqualTo("key0"),
                        kvMessage -> assertThat(kvMessage.getKey()).isEqualTo("key1"),
                        kvMessage -> assertThat(kvMessage.getKey()).isEqualTo("key2")
                ).extracting(KVMessage::getValue)
                .isNull();
    }


    @Test
    void putReturnsCorrectKeysAndValues() {
        final Cache cache = getCache(5);
        assertThat(cache.put("myKey", "myValue"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "myKey",
                        "myValue",
                        KVMessage.StatusType.PUT_SUCCESS
                );
        assertThat(cache.put("myKey2", "myValue2"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "myKey2",
                        "myValue2",
                        KVMessage.StatusType.PUT_SUCCESS
                );
    }

    @Test
    void getReturnsCorrectKeysAndValues() {
        final Cache cache = getCache(4);
        cache.put("myKey", "myValue");
        cache.put("myKey2", "myValue2");
        assertThat(cache.get("myKey"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "myKey",
                        "myValue",
                        KVMessage.StatusType.GET_SUCCESS
                );
    }

    @Test
    void deletesKey() {
        final Cache cache = getCache(4);

        for (int i = 0; i < 3; i++) {
            cache.put("key" + i, "value" + i);
        }

        assertThat(cache.get("key1"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "key1",
                        "value1",
                        KVMessage.StatusType.GET_SUCCESS
                );
        assertThat(cache.put("key1", null))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "key1",
                        null,
                        KVMessage.StatusType.DELETE_SUCCESS
                );
        assertThat(cache.get("key1"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus
                ).containsExactly(
                        "key1",
                        null,
                        KVMessage.StatusType.GET_ERROR
                );
    }

}
