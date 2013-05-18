package controller;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class MailControllerQueueSizeThread implements Runnable
{
	private final ScheduledExecutorService threadPool;
	private final MailController mailStorageQueue;
	private final long delay;
	private final TimeUnit timeUnit;
	
	private int lastSize = 0;
	
	public MailControllerQueueSizeThread(final ScheduledExecutorService threadPool, final MailController mailStorageQueue, final long delay, final TimeUnit timeUnit)
	{
		this.threadPool = threadPool;
		this.mailStorageQueue = mailStorageQueue;
		this.delay = delay;
		this.timeUnit = timeUnit;
	}
	
	@Override
	public void run()
	{
		final int newSize = mailStorageQueue.size();
		final int diff = newSize - lastSize;

		System.out.println("Mail queue size: " + newSize + " (" + (diff > 0 ? "+" : "") + diff + ")");
		lastSize = newSize;
		
		threadPool.schedule(this, delay, timeUnit);
	}
}
