package com.bdec.training.spark.corejava.java_threading;

import java.util.concurrent.atomic.AtomicInteger;

class UnsafeCounter {
    private static int counter = 0;
    private AtomicInteger safeCounter = new AtomicInteger(0);
    private StringBuffer stringBuffer = new StringBuffer();
    public void add(int value) {
         {
            this.counter = this.counter + value;
        }
        ///add some other logic...
        safeCounter.addAndGet(value);
    }
    public Integer getCounter() {
        return counter;
    }

    public synchronized void yetAnotherUnsafeMethod(String value) {
        stringBuffer.append(value);
    }
}

public class ThreadedCounter {
    public static void main(String[] args) {
        UnsafeCounter unsafeCounter = new UnsafeCounter();
        UnsafeCounter unsafeCounter1 = new UnsafeCounter();

        Thread T1 = new Thread("T1") {
            @Override
            public void run() {
                unsafeCounter.add(5);
            }
        };
        Thread T2 = new Thread("T2") {
            @Override
            public void run() {
                unsafeCounter1.add(10);
            }
        };

        T1.start();
        T2.start();
        try {
            T1.join();
            T2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(unsafeCounter.getCounter()); //output can be any of 5, 10 and 15
    }
}
