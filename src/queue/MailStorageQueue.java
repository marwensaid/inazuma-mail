package queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import com.couchbase.client.CouchbaseClient;

public class MailStorageQueue
{
	private final int numberOfThreads;
	private final MailStorageQueueThreads[] threads;
	private final CountDownLatch latch;

	public MailStorageQueue(final CouchbaseClient client, final int maxRetries, final int numberOfThreads)
	{
		this.numberOfThreads = numberOfThreads;
		this.threads = new MailStorageQueueThreads[numberOfThreads];
		this.latch = new CountDownLatch(numberOfThreads);
		
		for (int i = 0; i < numberOfThreads; i++)
		{
			threads[i] = new MailStorageQueueThreads(this, i, client, maxRetries);
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
			final long start = System.nanoTime();
			final long limit = TimeUnit.MINUTES.toNanos(60);
			while ((System.nanoTime() - start) < limit)
			{
				System.out.println("Mail queue size: " + size());
				latch.await(5, TimeUnit.SECONDS);
				if (latch.getCount() == 0)
				{
					return;
				}
			}
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
