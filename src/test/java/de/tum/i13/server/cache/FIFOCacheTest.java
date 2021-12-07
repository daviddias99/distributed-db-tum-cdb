package de.tum.i13.server.cache;

import org.junit.jupiter.api.Test;

import de.tum.i13.server.kv.KVMessage;

import static org.assertj.core.api.Assertions.assertThat;

class FIFOCacheTest extends CacheTest {

    @Override
    Cache getCache(int size) {
        return new FIFOCache(size);
    }

    @Test
    void displaceKeyFIFO() {
        final Cache cache = new FIFOCache(3);

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
                        null,
                        KVMessage.StatusType.GET_ERROR
                );
        assertThat(cache.get("key1"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "key1",
                        "value1",
                        KVMessage.StatusType.GET_SUCCESS
                );
    }

}