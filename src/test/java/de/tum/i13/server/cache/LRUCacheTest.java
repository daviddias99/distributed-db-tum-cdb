package de.tum.i13.server.cache;

import org.junit.jupiter.api.Test;

import de.tum.i13.shared.kv.KVMessage;

import static org.assertj.core.api.Assertions.assertThat;

class LRUCacheTest extends CacheTest {

    @Override
    Cache getCache(int size) {
        return new LRUCache(size);
    }

    @Test
    void displaceKeyLRU() {
        final LRUCache cache = new LRUCache(3);

        for (int i = 0; i < 3; i++) {
            cache.put("key" + i, "value" + i);
        }

        cache.get("key0");
        cache.put("key4", "value4");

        assertThat(cache.get("key0"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "key0",
                        "value0",
                        KVMessage.StatusType.GET_SUCCESS
                );
        assertThat(cache.get("key1"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "key1",
                        null,
                        KVMessage.StatusType.GET_ERROR
                );
    }

}