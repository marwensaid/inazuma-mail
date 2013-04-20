package queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.internal.OperationFuture;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.couchbase.client.CouchbaseClient;

class MailStorageQueueThreads extends Thread
{
	private final AtomicBoolean running = new AtomicBoolean(true);
	private final BlockingQueue<SerializedMail> incomingQueue;
	private final IntObjectOpenHashMap<ReceiverLookupDocument> lookupMap;
	private final int threadNo;
	private final MailStorageQueue mailStorageQueue;
	private final CouchbaseClient client;
	private final int maxRetries;
	
	private int maxTriesUsed = 0;
	
	protected MailStorageQueueThreads(final MailStorageQueue mailStorageQueue, final int threadNo, final CouchbaseClient client, final int maxRetries)
	{
		this.incomingQueue = new LinkedBlockingQueue<SerializedMail>();
		this.lookupMap = new IntObjectOpenHashMap<ReceiverLookupDocument>();
		this.threadNo = threadNo;
		this.mailStorageQueue = mailStorageQueue;
		this.client = client;
		this.maxRetries = maxRetries;
	}

	@Override
	public void run()
	{
		while (running.get() || incomingQueue.size() > 0)
		{
			try
			{
				final SerializedMail mail = incomingQueue.take();
				if (mail != null && !(mail instanceof PoisionedSerializedMail))
				{
					if (updateReceiverLookupDocument(mail))
					{
						if (!persistMail(mail))
						{
							System.err.println("Could not persist mail_" + mail.getKey() + " for receiver " + mail.getReceiverID());
						}
					}
				}
			}
			catch (InterruptedException e)
			{
				System.err.println("Could not take mail from queue: " + e.getMessage());
			}
		}
		System.out.println("# " + threadNo + " Max tries used: " + maxTriesUsed);
		mailStorageQueue.countdown();
	}
	
	protected void addMail(final SerializedMail mail)
	{
		incomingQueue.add(mail);
	}
	
	protected int size()
	{
		return incomingQueue.size();
	}
	
	protected void shutdown()
	{
		running.set(false);
		incomingQueue.add(new PoisionedSerializedMail());
	}
	
	private boolean updateReceiverLookupDocument(final SerializedMail mail)
	{
		final int receiverID = mail.getReceiverID();
		final String lookupDocumentKey = "receiver_" + receiverID;

		// Get lookup document
		ReceiverLookupDocument mailReceiverLookup = lookupMap.get(receiverID);
		if (mailReceiverLookup == null)
		{
			Object receiverLookupDocumentObject = null;
			boolean retry = true;
			int tries = 0;
			while (retry && ++tries < maxRetries)
			{
				if (tries > maxTriesUsed)
				{
					maxTriesUsed = tries;
				}
				retry = false;
				try
				{
					receiverLookupDocumentObject = client.get(lookupDocumentKey);
				}
				catch (Exception e)
				{
					retry = true;
					System.err.println("Could not read lookup document for " + receiverID + ": " + e.getMessage());
					threadSleep(tries * 5);
				}
			}
			if (retry)
			{
				System.err.println("Could not read lookup document for " + receiverID + " (permanently)");
				return false;
			}
			else if (receiverLookupDocumentObject != null)
			{
				mailReceiverLookup = ReceiverLookupDocument.fromJSON((String)receiverLookupDocumentObject);
			}
			else
			{
				mailReceiverLookup = new ReceiverLookupDocument();
			}
			lookupMap.put(receiverID, mailReceiverLookup);
		}
			
		// Modify lookup document
		if (!mailReceiverLookup.add(mail.getCreated(), mail.getKey()))
		{
			System.err.println("Could not add mail with same key for mail " + mail.getKey());
			return false;
		}
		
		// Store lookup document
		final String document = mailReceiverLookup.toJSON();
		int tries = 0;
		while (++tries < maxRetries)
		{
			if (tries > maxTriesUsed)
			{
				maxTriesUsed = tries;
			}
			try
			{
				OperationFuture<Boolean> replaceFuture = client.set(lookupDocumentKey, 0, document);
				if (replaceFuture.get())
				{
					return true;
				}
			}
			catch (Exception e)
			{
				System.err.println("Could not set lookup document for receiver " + receiverID + ": " + e.getMessage());
			}
		}
		System.err.println("Could not set lookup document for receiver " + receiverID + " (permanently)");
		return false;
	}
	
	private boolean persistMail(final SerializedMail mail)
	{
		int tries = 0;
		while (++tries < maxRetries)
		{
			if (tries > maxTriesUsed)
			{
				maxTriesUsed = tries;
			}
			try
			{
				OperationFuture<Boolean> addSuccess = client.add("mail_" + mail.getKey(), 0, mail.getDocument());
				if (addSuccess.get())
				{
					return true;
				}
			}
			catch (Exception e)
			{
				System.err.println("Could not add mail for receiver " + mail.getReceiverID() + ": " + e.getMessage());
				threadSleep(tries * 5);
			}
		}
		System.err.println("Could not add mail for receiver " + mail.getReceiverID() + " (permanently)");
		return false;
	}
	
	private void threadSleep(final long milliseconds)
	{
		try
		{
			Thread.sleep(milliseconds);
		}
		catch (InterruptedException e)
		{
		}
	}
	
	private class PoisionedSerializedMail extends SerializedMail
	{
		public PoisionedSerializedMail()
		{
			super(0, 0, null, null);
		}
	}
}
