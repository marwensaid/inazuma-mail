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
			threads[i] = new MailStorageQueueThreads(this, client, maxRetries);
			threads[i].start();
		}
	}
	
	public void addMail(SerializedMail mail)
	{
		final int threadNumber = mail.getReceiverID() % numberOfThreads;
		threads[threadNumber].addMail(mail);
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
