package com.incarcloud.rooster.gather;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestAsync {

	@Test
	public void testThreadPool() {

		try {
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

			System.out.println("KKKKKKKKKKKKKKKKKKKKK");
//			System.in.read();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
