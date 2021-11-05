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
    void displaceKeyWhenFull() {
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

}
