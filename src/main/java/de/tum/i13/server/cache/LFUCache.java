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

/**
 * A {@link Cache} with the {@link CachingStrategy} {@link CachingStrategy#LFU}
 */
public class LFUCache implements Cache {

    private static final Logger LOGGER = LogManager.getLogger(LFUCache.class);
    /**
     * Map of keys to nodes storing information about value and index in access frequency list
     */
    private final Map<String, MapNode> keyNodeMap;
    /**
     * List sorted in descending order according to access frequency
     */
    private final List<ListNode> accessFrequencyList;
    /**
     * The maximum number of entries in this cache
     */
    private final int size;

    /**
     * Constructs a cache with the given size
     *
     * @param size the size of the cache, must be greater than 0
     */
    public LFUCache(int size) {
        Preconditions.check(size > 0, "Cache must have a size greater than 0");

        this.keyNodeMap = new HashMap<>(size);
        this.accessFrequencyList = new ArrayList<>(size);
        this.size = size;
    }

    @Override
    public synchronized KVMessage get(String key) {
        Preconditions.notNull(key, "Key cannot be null");
        LOGGER.info("Trying to get value of key {}", key);

        return Optional.ofNullable(keyNodeMap.get(key))
                .map(value -> updateKeyFrequency(key, value))
                .orElseGet(() -> new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR));
    }

    /**
     * Increase the frequency of the key and potentially move it forward in the frequency list
     */
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
        LOGGER.info("Trying to put key {} with value {}", key, value);

        return Optional.ofNullable(value)
                .map(newValue -> putKeyToValue(key, newValue))
                .orElseGet(() -> deleteKey(key));

    }

    /**
     * Updates the key in the cache with an actual present value
     */
    private KVMessage putKeyToValue(String key, String value) {
        LOGGER.debug("Putting key {} to value {}", key, value);
        return Optional.ofNullable(keyNodeMap.get(key))
                .map(mapNode -> putPresentKey(key, value, mapNode))
                .orElseGet(() -> putAbsentKey(key, value));
    }

    /**
     * Deletes a key in the cache because of an absent value
     * Shifts all the keys following the key to delete in the frequency list one position to the left
     */
    private KVMessage deleteKey(String key) {
        LOGGER.debug("Deleting key {}", key);

        final Optional<MapNode> optMapNode = Optional.ofNullable(keyNodeMap.get(key));
        if (optMapNode.isEmpty()) {
            LOGGER.debug("Skipping deleting already absent key {}", key);
            return new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
        } else {
            final MapNode mapNode = optMapNode.get();
            LOGGER.debug("Deleting key {} with previous value {}", key, mapNode.value);
            keyNodeMap.remove(key);
            final ListIterator<ListNode> iterator = accessFrequencyList.listIterator(mapNode.listIndex);
            iterator.next();
            while (iterator.hasNext()) {
                final int nextIndex = iterator.nextIndex();
                accessFrequencyList.set(nextIndex - 1, accessFrequencyList.get(nextIndex));
                keyNodeMap.get(accessFrequencyList.get(nextIndex - 1).key).listIndex = nextIndex - 1;
                iterator.next();
            }
            iterator.remove();
            return new KVMessageImpl(key, KVMessage.StatusType.DELETE_SUCCESS);
        }
    }

    /**
     * Updates a key in the cache, if the key was already present
     */
    private KVMessage putPresentKey(String key, String value, MapNode mapNode) {
        LOGGER.debug("Putting key {} with previously present value {} to value {}", key, mapNode.value, value);
        mapNode.value = value;
        updateKeyFrequency(key, mapNode);
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
    }

    /**
     * Updates a key in the cache, if it was not present before
     * Replaces the key last in the list, if the cache was already full, otherwise is appended to end
     */
    private KVMessageImpl putAbsentKey(String key, String value) {
        LOGGER.debug("Putting key {} with previously absent value to value {}", key, value);
        if (accessFrequencyList.size() < size) {
            LOGGER.debug("Putting key {} to value {} in non-full cache", key, value);
            keyNodeMap.put(key, new MapNode(value, accessFrequencyList.size()));
            accessFrequencyList.add(new ListNode(key));
        } else {
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

}
