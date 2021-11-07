package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

public class LFUCache implements Cache {

    private static class MapNode {

        private String value;
        private int listIndex;

        private MapNode(String value, int listIndex) {
            this.value = value;
            this.listIndex = listIndex;
        }

    }

    private static class ListNode {

        private final String key;
        private int accessFrequency;

        private ListNode(String key) {
            this.key = key;
            this.accessFrequency = 0;
        }

        private void incrementFrequency() {
            this.accessFrequency++;
        }

    }


    private final Map<String, MapNode> keyNodeMap;
    /**
     * List sorted in descending order according to access frequency
     */
    private final List<ListNode> accessFrequencyList;
    /**
     * The maximum number of entries in this cache
     */
    private final int maxEntries;

    private static final Logger LOGGER = LogManager.getLogger(LFUCache.class);

    public LFUCache(int maxEntries) {
        Preconditions.check(maxEntries > 0, "Cache must have a size greater than 0");

        this.keyNodeMap = new HashMap<>(maxEntries);
        this.accessFrequencyList = new ArrayList<>(maxEntries);
        this.maxEntries = maxEntries;
    }

    @Override
    public synchronized KVMessage get(String key) {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        return Optional.ofNullable(keyNodeMap.get(key))
                .map(value -> updateKeyFrequency(key, value))
                .orElse(new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR));
    }

    private KVMessage updateKeyFrequency(String key, MapNode mapNode) {
        LOGGER.debug("Updating frequency of key {}", key);

        final int currentIndex = mapNode.listIndex;
        final ListNode currentListNode = accessFrequencyList.get(currentIndex);

        currentListNode.incrementFrequency();

        final ListIterator<ListNode> iterator = accessFrequencyList.listIterator(currentIndex);
        if (iterator.hasPrevious()) {
            final int previousIndex = iterator.previousIndex();
            final ListNode previousListNode = iterator.previous();
            if (previousListNode.accessFrequency < currentListNode.accessFrequency) {
                LOGGER.debug("Moving key {} forward in list due to updated frequency", key);
                accessFrequencyList.set(previousIndex, currentListNode);
                accessFrequencyList.set(currentIndex, previousListNode);
                keyNodeMap.get(currentListNode.key).listIndex = previousIndex;
                keyNodeMap.get(previousListNode.key).listIndex = currentIndex;
            }
        }

        return new KVMessageImpl(key, mapNode.value, KVMessage.StatusType.GET_SUCCESS);
    }

    @Override
    public synchronized KVMessage put(String key, String value) {
        Preconditions.notNull(key, "Key cannot be null");
        Preconditions.notNull(value, "Value cannot be null");
        LOGGER.info("Trying to put key {} with value {}", key, value);

        return Optional.ofNullable(keyNodeMap.get(key))
                .map(mapNode -> putPresentKey(key, value, mapNode))
                .orElse(putAbsentKey(key, value));
    }

    private KVMessage putPresentKey(String key, String value, MapNode mapNode) {
        LOGGER.debug("Putting key {} with previously present value {} to value {}", key, mapNode.value, value);
        mapNode.value = value;
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
    }

    private KVMessageImpl putAbsentKey(String key, String value) {
        LOGGER.debug("Putting key {} with previously absent value to value {}", key, value);
        if (accessFrequencyList.size() < maxEntries) {
            LOGGER.debug("Putting key {} to value {} in non-full cache", key, value);
            keyNodeMap.put(key, new MapNode(value, accessFrequencyList.size()));
            accessFrequencyList.add(new ListNode(key));
        }
        else {
            LOGGER.debug("Putting key {} to value {} in full cache", key, value);
            final int indexLFU = accessFrequencyList.size() - 1;
            keyNodeMap.remove(accessFrequencyList.get(indexLFU).key);
            keyNodeMap.put(key, new MapNode(value, indexLFU));
            accessFrequencyList.set(indexLFU, new ListNode(key));
        }
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
    }

    @Override
    public CachingStrategy getCachingStrategy() {
        return CachingStrategy.LFU;
    }

}
