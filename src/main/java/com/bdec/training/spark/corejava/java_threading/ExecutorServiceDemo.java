package com.bdec.training.spark.corejava.java_threading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

class DemoReturnCallable implements Callable<String> {

    @Override
    public String call() throws Exception {
        return "CallableDemoString";
    }
}

public class ExecutorServiceDemo {
    public static void simple_executor_service() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<MyRunnable> objs = new ArrayList<>();
        for(int i = 0; i < 10; i++) {
            MyRunnable obj = new MyRunnable(" MyRunnabble " + i);
            objs.add(obj);
            executorService.execute(obj);
        }

        System.out.println("Main thread: " + Thread.currentThread().getName() +
                " thread id " + Thread.currentThread().getId());

        for(MyRunnable obj : objs) {
            System.out.println(obj.getReturnVariable());
        }

        executorService.shutdown();
    }

    public static void executor_service_with_multi_futures_and_runnable() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        List<Future> futures = new ArrayList<>();
        for(int i = 0; i < 10; i++ ) {
            Future future = executorService.submit(new MyRunnable("Future task " + i));
            futures.add(future);
        }

        try {
            //future.get();
            Future future = futures.get(1);
            future.get(1, TimeUnit.SECONDS);
            System.out.println("future 1 has completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }

    public static void executor_service_with_futures_and_runnable() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Future future = executorService.submit(new Runnable() {
            public void run() {
                System.out.println("Asynchronous task");
            }
        });

        try {
            //future.get();
            future.get(100, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }


    public static void executor_service_with_futures_and_callable() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Future<String> future1 = executorService.submit(new Callable<String>() {
            public String call() {
                System.out.println("Asynchronous task 1");
                return "Task 1";
            }
        });

        try {
            String callableResponse = future1.get();
            System.out.println("Return from callable service " + callableResponse);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }

    public static void executor_service_with_multiple_callable() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<Callable<String>> callables = new ArrayList<>();
        for(int i = 0; i < 5; i++) {
            int finalI = i;
            callables.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    String mesg = "Return from callable " + finalI;
                    System.out.println("Finished callable " + finalI);
                    Thread.sleep(100);
                    return mesg;
                }
            });
        }

        List<Future<String>> futures = null;
        try {
            futures = executorService.invokeAll(callables);
            //Future<String> f2 = futures.get(1);
//            String call2ReturnValue = f2.get();
//            System.out.println(call2ReturnValue);

            for(Future<String> future : futures){
                if(future.isDone())
                    System.out.println("future.get = " + future.get());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }


    public static void main(String[] args) {
        //simple_executor_service();
        //executor_service_with_multi_futures_and_runnable();
        //executor_service_with_futures_and_runnable();
        //executor_service_with_futures_and_callable();
        executor_service_with_multiple_callable();
    }
}
