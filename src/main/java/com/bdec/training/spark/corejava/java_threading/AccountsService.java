package com.bdec.training.spark.corejava.java_threading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Accounts {
    private String accountId;
    private Integer balance;

    public synchronized void operate(String operation, Integer value) {
        if(operation.equals("credit")) {
            balance = balance + value;
        } else {
            balance = balance - value;
        }
    }

    public synchronized String operate_with_return(String operation, Integer value) {
        if(operation.equals("credit")) {
            balance = balance + value;
        } else {
            balance = balance - value;
        }

        if(balance < 0)
            return accountId + ":negative balance";
        else
            return accountId+ ":okay";
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Integer getBalance() {
        return balance;
    }

    public void setBalance(Integer balance) {
        this.balance = balance;
    }
}



public class AccountsService {
    private static AtomicInteger maxTrans = new AtomicInteger(0);

    public static void setMaxTrans(Integer value) {
        if(maxTrans.get() < value) {
            maxTrans.set(value);
        }
    }

    public static Runnable get_operation() {
        Accounts dummy = new Accounts(); //somehow we got the account object
        String dummyOperation = "somehow got the operation";
        Integer value = 0;
        return new Runnable() {
            @Override
            public void run() {
                AccountsService.setMaxTrans(value);
                dummy.operate(dummyOperation, value);
            }
        };
    }

    public static Callable<String> get_operation_callable() {
        Accounts dummy = new Accounts(); //somehow we got the account object
        String dummyOperation = "somehow got the operation";
        Integer value = 0;
        return new Callable<String>() {
            @Override
            public String call() {
                AccountsService.setMaxTrans(value);
                return dummy.operate_with_return(dummyOperation, value);
            }
        };
    }

    public static void simple_executor_service() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<String>> futureList = new ArrayList<>();
        //getting account operations somewhere
        while(true) {
            //Runnable obj = get_operation();
            //executorService.execute(obj);
            Callable<String> callable = get_operation_callable();
            Future<String> f = executorService.submit(callable);
            futureList.add(f);

            for(Future<String> doneFuture : futureList) {
                if(doneFuture.isDone()) {
                    try {
                        String ret = doneFuture.get();
                        //do something with ret
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
