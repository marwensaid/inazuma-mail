package main;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import queue.MailStorageQueue;

public class MailQueueSizeJob implements Runnable
{
	private final ScheduledExecutorService threadPool;
	private final MailStorageQueue mailStorageQueue;
	
	public MailQueueSizeJob(final ScheduledExecutorService threadPool, final MailStorageQueue mailStorageQueue)
	{
		this.threadPool = threadPool;
		this.mailStorageQueue = mailStorageQueue;
	}
	
	@Override
	public void run()
	{
		if (threadPool.isShutdown() || threadPool.isTerminated())
		{
			return;
		}
		
		System.out.println("Mail queue size: " + mailStorageQueue.size());
		
		if (!threadPool.isShutdown() && !threadPool.isTerminated())
		{
			threadPool.schedule(this, 5, TimeUnit.SECONDS);
		}
	}
}
