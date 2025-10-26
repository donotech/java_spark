package com.bdec.training.spark.corejava.java_threading;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class CompletableFutureDemo {
    static ExecutorService executorService = Executors.newFixedThreadPool(10);

    static void cfExample() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> cf = CompletableFuture.runAsync(()->{
            System.out.println("Running a runnable task cf1 " + Thread.currentThread());
        }, executorService);
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()->{
            System.out.println("Running a runnable task cf2 " + Thread.currentThread());
        }, executorService);
        System.out.println("Returned Value- " + cf.get());
        System.out.println("Returned Value- " + cf2.get());
    }

    static void cfSupplyExample() throws InterruptedException, ExecutionException {
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(()->{
            System.out.println("Running a task 1");
            return "Task Completed 1 " + Thread.currentThread();
        }, executorService);

        CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(()->{
            System.out.println("Running a task 2");
            return "Task Completed 2 " + Thread.currentThread();
        }, executorService);

        System.out.println("Returned Value- " + cf.get());
        System.out.println("Returned Value- " + cf2.get());
    }

    static void cfThenApplyExample() throws InterruptedException, ExecutionException {
        StringBuilder sb = new StringBuilder();
        CompletableFuture<String> cf = CompletableFuture
                .supplyAsync(()->{
            return "Completable " + Thread.currentThread();
        }, executorService)
                .thenApplyAsync(s->sb.append(s).append(" Future " + Thread.currentThread()).toString(), executorService);
        System.out.println("Returned Value- " + cf.get());

    }

    static void cfExampleCombine() throws InterruptedException, ExecutionException {
        CompletableFuture<String> cf = getHello().thenCombineAsync(getWorld(),
                (s1, s2)->s1+ " " +s2, executorService);
        System.out.println("Value- " + cf.get());
    }

    static void end_foo() {

    }
    static void foo() {
//        CompletableFuture<String> task1;
//        CompletableFuture<String> task2;
//        CompletableFuture<String> task3;
//        CompletableFuture.allOf(task1,task2,task3).whenComplete(new BiConsumer<Void, Throwable>() {
//            @Override
//            public void accept(Void unused, Throwable throwable) {
//               task1.get();
//            }
//        });

    }

    void foo_atomic() {
        AtomicInteger i = new AtomicInteger();

    }

    static CompletableFuture<String> getHello(){
        return CompletableFuture.supplyAsync(()->{return "Hello";}, executorService);
    }

    static CompletableFuture<String> getWorld(){
        return CompletableFuture.supplyAsync(()->{return "World";}, executorService);
    }

    public static void main(String[] args) {
        try {
//            cfExample();
//            cfSupplyExample();
            cfThenApplyExample();
//            cfThenApplyExample();
//            cfExampleCombine();
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
