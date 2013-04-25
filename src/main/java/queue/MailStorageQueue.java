package queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import util.NamedThreadFactory;


import com.couchbase.client.CouchbaseClient;

public class MailStorageQueue
{
	private final int numberOfThreads;
	private final MailStorageQueueInsertThread[] threads;
	private final CountDownLatch latch;
	private final ScheduledExecutorService threadPool;
	private final Future<?> queueSizeThread;
	
	public MailStorageQueue(final CouchbaseClient client, final int numberOfThreads, final int maxRetries)
	{
		this.numberOfThreads = numberOfThreads;
		this.threads = new MailStorageQueueInsertThread[numberOfThreads];
		this.latch = new CountDownLatch(numberOfThreads);
		this.threadPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("MailStorageQueueSizeThread"));
		
		queueSizeThread = threadPool.submit(new MailStorageQueueSizeThread(threadPool, this, 5, TimeUnit.SECONDS));
		for (int i = 0; i < numberOfThreads; i++)
		{
			threads[i] = new MailStorageQueueInsertThread(this, i + 1, client, maxRetries);
			threads[i].start();
		}
	}
	
	public void addMail(SerializedMail mail)
	{
		final int threadNumber = mail.getReceiverID() % numberOfThreads;
		threads[threadNumber].addMail(mail);
	}
	
	public int size()
	{
		int size = 0;
		for (int i = 0; i < numberOfThreads; i++)
		{
			size += threads[i].size();
		}
		return size;
	}

	public void shutdown()
	{
		for (int i = 0; i < numberOfThreads; i++)
		{
			threads[i].shutdown();
		}
	}
	
	public void awaitShutdown()
	{
		try
		{
			latch.await(60, TimeUnit.MINUTES);
			queueSizeThread.cancel(true);
			threadPool.shutdown();
		}
		catch (InterruptedException e)
		{
		}
	}

	protected void countdown()
	{
		latch.countDown();
	}
}
