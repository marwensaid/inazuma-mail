package main;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import queue.MailStorageQueue;

public class MailQueueSizeJob implements Runnable
{
	private final ScheduledExecutorService threadPool;
	private final MailStorageQueue mailStorageQueue;
	
	private int lastSize;
	
	public MailQueueSizeJob(final ScheduledExecutorService threadPool, final MailStorageQueue mailStorageQueue)
	{
		this.threadPool = threadPool;
		this.mailStorageQueue = mailStorageQueue;
		
		lastSize = -1;
	}
	
	@Override
	public void run()
	{
		if (threadPool.isShutdown() || threadPool.isTerminated())
		{
			return;
		}
		
		int newSize = mailStorageQueue.size();
		if (lastSize > 0)
		{
			System.out.println("Mail queue size: " + newSize);
		}
		else
		{
			int diff = lastSize - newSize;
			System.out.println("Mail queue size: " + newSize + " (" + (diff > 0 ? "+" : "") + diff + ")");
		}
		lastSize = newSize;
		
		if (!threadPool.isShutdown() && !threadPool.isTerminated())
		{
			threadPool.schedule(this, 5, TimeUnit.SECONDS);
		}
	}
}
