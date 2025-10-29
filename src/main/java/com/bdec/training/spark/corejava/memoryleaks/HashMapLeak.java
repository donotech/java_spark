package com.bdec.training.spark.corejava.memoryleaks;

import java.util.HashMap;
import java.util.Map;

public class HashMapLeak {
    private Map<Key, String> map = new HashMap<>();

    static class Key {
        private String id;

        public Key(String id) { this.id = id; }

        // Missing equals() and hashCode()
        // Can't find/remove entries properly
    }

    public void demo() {
        Key k1 = new Key("1");
        map.put(k1, "value");

        Key k2 = new Key("1"); // Different object, same logical key
        map.remove(k2); // Won't remove k1, leak!
    }
}