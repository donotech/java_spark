package com.bdec.training.spark.corejava.memoryleaks;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class CacheLeak {
    private Map<String, Object> cache = new HashMap<>();

    private Object expensiveOperation(String key) {
        String someReference = key; ///assume a jdbc or some other calls here
        return someReference;
    }

    public Object get(String key) {
        if (!cache.containsKey(key)) {
            Object value = expensiveOperation(key);
            cache.put(key, value); // Cache grows forever
        }
        return cache.get(key);
    }

    // Better approach - use bounded cache:
    private Map<String, Object> betterCache =
            new LinkedHashMap<String, Object>(100, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > 100;
                }
            };
}

class CustomStackLeak {
    private Object[] elements;
    private int size = 0;

    public CustomStackLeak(int capacity) {
        elements = new Object[capacity];
    }

    public void push(Object obj) {
        elements[size++] = obj;
    }

    // Memory leak version:
    public Object popLeaky() {
        return elements[--size]; // Old reference still in array!
    }

    // Correct version:
    public Object pop() {
        Object result = elements[--size];
        elements[size] = null; // Clear reference
        return result;
    }
}

public class UnboundedMemoryLeaks {

}
