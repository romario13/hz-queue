package ru.romario.hzqueue.oom;

import com.hazelcast.core.QueueStore;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 */
public class MemoryQueueStore implements QueueStore<Integer> {

    private HashMap<Long, Integer> store = new HashMap<Long, Integer>();

    @Override
    public void store(Long key, Integer value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Long, Integer> map) {
        store.putAll(map);
    }

    @Override
    public void delete(Long key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        for (Long key : keys) {
            delete(key);
        }
    }

    @Override
    public Integer load(Long key) {
        return store.get(key);
    }

    @Override
    public Map<Long, Integer> loadAll(Collection<Long> keys) {
        HashMap<Long, Integer> partOfStore = new HashMap<Long, Integer>();

        for (Long key : keys) {
            partOfStore.put(key, store.get(key));
        }

        return partOfStore;
    }

    @Override
    public Set<Long> loadAllKeys() {
        SortedSet sortedSet = new TreeSet();
        sortedSet.addAll(store.keySet());

        return Collections.unmodifiableSortedSet(sortedSet);
    }
}
