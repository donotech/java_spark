package com.bdec.training.spark.corejava.lamdba;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

interface FunctionalFoo {
    public void bar(String s, String t1);
}

public class Scratch {
    public static void main(String[] args) {
        List<String> l = Arrays.asList("hello", "world", "how", "are", "things");
        Collections.sort(l, new Comparator<String>() {
            @Override
            public int compare(String s, String t1) {
                return s.compareTo(t1);
            }
        });

        Collections.sort(l, (s, t1) -> s.compareTo(t1));
    }
}
