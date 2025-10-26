package com.bdec.training.spark.corejava.java_threading;

class MyRunnable implements Runnable {
    String runnableName;

    public MyRunnable(String runnableName) {
        this.runnableName = runnableName;
    }

    String returnVariable;

    public String getReturnVariable() {
        return runnableName + ":" + returnVariable;
    }

    @Override
    public void run() {
        this.returnVariable = "This is returned from " + runnableName + ":thread name:" +
                Thread.currentThread().getName() + ":" + Thread.currentThread().getId();
        System.out.println(this.returnVariable);
    }
}

public class MyThread extends Thread {

    @Override
    public void run(){
        System.out.println("MyThread running " + Thread.currentThread().getName() + " my id " + getId());
    }

    public static void main(String[] args) {
        //runnable with no thread
//        MyRunnable r = new MyRunnable("No Thread");
//        r.run();
//
        MyRunnable runnable = new MyRunnable("My Runnable Thread");
        Thread runnableThread = new Thread(runnable, "Runnable thread");
        runnableThread.start();
////
////        for(int i = 0; i < 3; i++) {
////            System.out.println("In main thread i = " + i);
////            try {
////                Thread.sleep(100);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
////        }
        MyThread myThread = new MyThread();
        myThread.start();
        System.out.println("Print im in main thread");
//
        Thread  namedThread = new Thread("My name is Thread") {
            public void run(){
                System.out.println("run by: " + getName());
            }
        };

        namedThread.start();
        try {
            myThread.join(10l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("After myThread");

//        System.out.println("Current name : " + Thread.currentThread().getName());
//
//
//
//
//
//        Thread  infiniteThread = new Thread("My name is infinity") {
//            public void run(){
//                for(int i = 0; i<10; i++) {
//                    try {
//                        System.out.println("waiting: " + getName() + " i = " + i);
//                        Thread.sleep(1000L);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//                System.out.println("run by: " + getName());
//            }
//        };
//
//        infiniteThread.start();
//

    }
}
