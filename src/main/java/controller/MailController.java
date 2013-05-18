package controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import model.SerializedMail;
import stats.BasicStatisticValue;
import stats.CustomStatisticValue;

import com.couchbase.client.CouchbaseClient;

public class MailController
{
	private final CouchbaseClient client;
	private final int numberOfThreads;
	private final MailControllerQueueThread[] threads;
	private final CountDownLatch latch;

	private final BasicStatisticValue mailFetched = new BasicStatisticValue("MailService", "mailFetched");
	private final BasicStatisticValue mailAdded = new BasicStatisticValue("MailService", "mailAdded");
	private final BasicStatisticValue mailDeleted = new BasicStatisticValue("MailService", "mailDeleted");
	
	private final BasicStatisticValue lookupRetries = new BasicStatisticValue("MailService", "retriesLookup");
	private final BasicStatisticValue lookupPersisted = new BasicStatisticValue("MailService", "persistedLookup");
	
	private final BasicStatisticValue mailRetries = new BasicStatisticValue("MailService", "retriesMail");
	private final BasicStatisticValue mailPersisted = new BasicStatisticValue("MailService", "persistedMail");

	public MailController(final CouchbaseClient client, final int numberOfThreads, final int maxRetries)
	{
		this.client = client;
		this.numberOfThreads = numberOfThreads;
		this.threads = new MailControllerQueueThread[numberOfThreads];
		this.latch = new CountDownLatch(numberOfThreads);

		for (int i = 0; i < numberOfThreads; i++)
		{
			threads[i] = new MailControllerQueueThread(this, i + 1, client, maxRetries);
			threads[i].start();
		}

		new CustomStatisticValue<Integer>("MailService", "queueSize", new QueueSizeCollector(this));
	}

	public String getMailKeys(final int receiverID)
	{
		return threads[receiverID].getMailKeys(receiverID);
	}

	public String getMail(final String mailKey)
	{
		// TODO: Add exception handling
		mailFetched.increment();
		return String.valueOf(client.get("mail_" + mailKey));
	}

	public void addMail(final SerializedMail mail)
	{
		mailAdded.increment();
		threads[calculateThreadNumber(mail.getReceiverID())].addMail(mail);
	}

	public void deleteMail(final int receiverID, final String mailKey)
	{
		mailDeleted.increment();
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
		}
		catch (InterruptedException e)
		{
		}
	}

	protected void incrementLookupRetries()
	{
		lookupRetries.increment();
	}

	protected void incrementLookupPersisted()
	{
		lookupPersisted.increment();
	}

	protected void incrementMailRetries()
	{
		mailRetries.increment();
	}

	protected void incrementMailPersisted()
	{
		mailPersisted.increment();
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
