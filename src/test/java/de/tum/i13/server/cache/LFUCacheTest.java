package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LFUCacheTest extends CacheTest {

    @Override
    Cache getCache(int size) {
        return new LFUCache(size);
    }

    @Test
    void displayKeyLFU() {
        final Cache cache = new LFUCache(3);

        for (int i = 0; i < 3; i++) {
            cache.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 2; i++) {
            cache.get("key0");
            cache.get("key2");
        }
        cache.get("key2");
        cache.put("key3", "value3");

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
        assertThat(cache.get("key3"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "key3",
                        "value3",
                        KVMessage.StatusType.GET_SUCCESS
                );
    }

}