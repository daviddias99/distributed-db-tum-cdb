package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.Preconditions;

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

    public LFUCache(int maxEntries) {
        Preconditions.check(maxEntries > 0);

        this.keyNodeMap = new HashMap<>(maxEntries);
        this.accessFrequencyList = new ArrayList<>(maxEntries);
        this.maxEntries = maxEntries;
    }

    @Override
    public synchronized KVMessage get(String key) {
        Preconditions.notNull(key);

        final Optional<MapNode> optValue = Optional.ofNullable(keyNodeMap.get(key));
        if (optValue.isEmpty()) {
            return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
        }
        else {
            final MapNode mapNode = optValue.get();

            final int currentIndex = mapNode.listIndex;
            final ListNode currentListNode = accessFrequencyList.get(currentIndex);

            currentListNode.incrementFrequency();

            final ListIterator<ListNode> iterator = accessFrequencyList.listIterator(currentIndex);
            if (iterator.hasPrevious()) {
                final int previousIndex = iterator.previousIndex();
                final ListNode previousListNode = iterator.previous();
                if (previousListNode.accessFrequency < currentListNode.accessFrequency) {
                    accessFrequencyList.set(previousIndex, currentListNode);
                    accessFrequencyList.set(currentIndex, previousListNode);
                    keyNodeMap.get(currentListNode.key).listIndex = previousIndex;
                    keyNodeMap.get(previousListNode.key).listIndex = currentIndex;
                }
            }

            return new KVMessageImpl(key, mapNode.value, KVMessage.StatusType.GET_SUCCESS);
        }
    }

    @Override
    public synchronized KVMessage put(String key, String value) {
        Preconditions.notNull(key);
        Preconditions.notNull(value);

        final Optional<MapNode> optMapNode = Optional.ofNullable(keyNodeMap.get(key));

        if (optMapNode.isPresent()) {
            optMapNode.get().value = value;
            return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
        }
        else {
            if (accessFrequencyList.size() < maxEntries) {
                keyNodeMap.put(key, new MapNode(value, accessFrequencyList.size()));
                accessFrequencyList.add(new ListNode(key));
            }
            else {
                final int indexWithLeastAccesses = accessFrequencyList.size() - 1;
                keyNodeMap.remove(accessFrequencyList.get(indexWithLeastAccesses).key);
                keyNodeMap.put(key, new MapNode(value, indexWithLeastAccesses));
                accessFrequencyList.set(indexWithLeastAccesses, new ListNode(key));
            }
            return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
        }
    }

}
