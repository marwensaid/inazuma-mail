package mailstorage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import util.NamedThreadFactory;

import com.couchbase.client.CouchbaseClient;

public class MailStorage
{
	private final CouchbaseClient client;
	private final int numberOfThreads;
	private final MailStorageQueueThread[] threads;
	private final CountDownLatch latch;
	private final ScheduledExecutorService threadPool;
	private final Future<?> queueSizeThread;
	
	public MailStorage(final CouchbaseClient client, final int numberOfThreads, final int maxRetries)
	{
		this.client = client;
		this.numberOfThreads = numberOfThreads;
		this.threads = new MailStorageQueueThread[numberOfThreads];
		this.latch = new CountDownLatch(numberOfThreads);
		this.threadPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("MailStorageQueueSizeThread"));
		
		queueSizeThread = threadPool.submit(new MailStorageQueueSizeThread(threadPool, this, 5, TimeUnit.SECONDS));
		for (int i = 0; i < numberOfThreads; i++)
		{
			threads[i] = new MailStorageQueueThread(this, i + 1, client, maxRetries);
			threads[i].start();
		}
	}
	
	public String getMailKeys(final int receiverID)
	{
		return threads[receiverID].getMailKeys(receiverID);
	}

	public String getMail(final String mailKey)
	{
		// TODO: Add exception handling
		return String.valueOf(client.get("mail_" + mailKey));
	}
	
	public void addMail(final SerializedMail mail)
	{
		threads[calculateThreadNumber(mail.getReceiverID())].addMail(mail);
	}
	
	public void deleteMail(final int receiverID, final String mailKey)
	{
		threads[calculateThreadNumber(receiverID)].deleteMail(receiverID, mailKey);
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
	
	private int calculateThreadNumber(final int receiverID)
	{
		return receiverID % numberOfThreads;
	}
}
