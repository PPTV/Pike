package com.pplive.pike;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ConsoleSubmiterServerTest {
	@Test
	public void port() throws InterruptedException {
		System.out.println("start");
		for (int i = 0; i < 10; i++) {
			System.out.println("thread" + i);
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					ConsoleSubmiterServer server = new ConsoleSubmiterServer();
					System.out.println(server.getPort());
					server.waitForResult();

				}
			});
			thread.start();
		}
		Thread.sleep(100000);

	}
}
