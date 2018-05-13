package com.incarcloud.rooster.gather;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * AsyncTest
 */
public class AsyncTest {

    @Test
    public void testThreadPool() {
        ExecutorService threadPool = Executors.newFixedThreadPool(2);

//			ExecutorService threadPool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
//					new LinkedBlockingQueue<Runnable>(2));

        Runnable r = new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("rrrrrrrrrrr");
            }
        };

        threadPool.execute(r);
        threadPool.execute(r);
        threadPool.execute(r);
        threadPool.execute(r);
        threadPool.execute(r);
        threadPool.execute(r);
        threadPool.execute(r);

        System.out.println("--end");
    }
}
