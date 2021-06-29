package com.xupt.thraed;

import java.util.UUID;
import java.util.concurrent.*;

/**
 * @author Wnlife
 */
public class ThreadTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService executorService = Executors.newFixedThreadPool(1, (Runnable r) -> {
            Thread t = new Thread(r, "my_thread");
            t.setDaemon(true);
            return t;
        });
        Future<String> future = CompletableFuture.supplyAsync(() -> {
            String name=UUID.randomUUID().toString();
            throw new RuntimeException("报错了。。。。");
//            return name;
        }, executorService);

        System.out.println(future.get());
    }

}
