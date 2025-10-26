package com.bdec.training.spark.corejava.java_threading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class Counter {
    private Integer counter = 0;
    private AtomicInteger atomicCounter = new AtomicInteger(0);
    private volatile int volatileCounter = 0;
    private ThreadLocal<Integer> threadLocalCounter = new ThreadLocal<>();

    public void add(int value) { //thread unsafe
        this.counter = this.counter + value;
    }

     public synchronized void addSync(int value) { //thread unsafe
        //somestatement() = this will also wait
         this.counter = this.counter + value;
     }

     public void addSafe(int value) { //thread safe
         this.atomicCounter.addAndGet(value);
     }

     public void addVolatile(int value) { //thread safe
        this.volatileCounter = this.volatileCounter + value;
     }

    public void addThreadLocal(int value) { //thread safe
        this.threadLocalCounter.set(threadLocalCounter.get()  + value);
    }
}


public class TwoSums {

    private int sum1 = 0;
    private int sum2 = 0;

    private Integer sum1Lock = new Integer(1);
    private Integer sum2Lock = new Integer(2);
    private AtomicInteger atomInt1 = new AtomicInteger(100);

    private static int myStaticVar = 0;
    List<Integer> syncList = Collections.synchronizedList(new ArrayList<Integer>()); //thread safe list

    public synchronized void changeStatic(int newValue) { //useless as it cant protect a static variable
        myStaticVar = newValue;
    }

    public synchronized void addList(List<Integer> val1) { //Thread T3 can change val1 while addList is called
        val1.add(sum1);
        val1.add(sum2);
    }

    public synchronized void addLockMethod(int val1, int val2){
        this.sum1 += val1;
        this.sum2 += val2;
    }

    public void changeValue(int valToChange) {
        this.sum1 = valToChange;
    }

    public void addLockAll(int val1, int val2){
        synchronized(this){
            this.sum1 += val1;
            this.sum2 += val2;
        }
    }

    public void add(int val1, int val2){
        synchronized(this.sum1Lock){
            this.sum1 += val1;
        }
        synchronized(this.sum2Lock){
            this.sum2 += val2;
        }
    }
}