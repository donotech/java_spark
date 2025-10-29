package com.bdec.training.spark.corejava.memoryleaks;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class ClassLoaderLeak {
    private static List<ClassLoader> loaders = new ArrayList<>();

    public void loadClass() throws Exception {
        URLClassLoader loader = new URLClassLoader(
                new URL[]{new URL("file:///path/to/jar")}
        );
        loaders.add(loader); // Loader never removed
        // All classes loaded by this loader can't be GC'd
    }
}