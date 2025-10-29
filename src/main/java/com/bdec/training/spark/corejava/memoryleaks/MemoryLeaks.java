package com.bdec.training.spark.corejava.memoryleaks;
import java.awt.*;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;



class StaticMemoryLeak {
    private static final List<Object> cache = new ArrayList<>();

    public void addToCache(Object obj) {
        cache.add(obj); // Objects never removed, keeps growing
    }
}

class ResourceLeak {
    public void readFile(String path) {
        try {
            FileInputStream fis = new FileInputStream(path);
            // Use stream but never close it
            // Should use try-with-resources
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // Correct approach:
    public void readFileCorrectly(String path) {
        try (FileInputStream fis = new FileInputStream(path)) {
            // Stream automatically closed
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

 class ListenerLeak {
    private Button button = new Button();

    public void setupListener() {
        ActionListener listener = e -> {
            // Handle event
        };
        button.addActionListener(listener);
        // Listener never removed, keeps reference to 'this'
    }

    // Correct approach:
    public void cleanup(ActionListener listener) {
        button.removeActionListener(listener);
    }
}

class ThreadLocalLeak {
    private static ThreadLocal<Integer> threadLocal = new ThreadLocal<>();

    public void doWork() {
        threadLocal.set(new Integer(42));
        // Work done, but threadLocal.remove() never called
        // In thread pools, thread is reused and object persists
    }

    // Correct approach:
    public void doWorkCorrectly() {
        try {
            threadLocal.set(new Integer(42));
            // Do work
        } finally {
            threadLocal.remove(); // Always clean up
        }
    }
}


public class MemoryLeaks {
    public static void main(String[] args) {

    }
}