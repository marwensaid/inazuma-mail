package queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.internal.OperationFuture;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.couchbase.client.CouchbaseClient;

class MailStorageQueueThread extends Thread
{
	private final static int RETRY_DELAY = 50;
	
	private final AtomicBoolean running = new AtomicBoolean(true);
	private final BlockingQueue<SerializedMail> incomingQueue;
	private final IntObjectOpenHashMap<ReceiverLookupDocument> lookupMap;
	private final IntOpenHashSet receiverOnQueue;
	private final int threadNo;
	private final MailStorageQueue mailStorageQueue;
	private final CouchbaseClient client;
	private final int maxRetries;
	
	protected MailStorageQueueThread(final MailStorageQueue mailStorageQueue, final int threadNo, final CouchbaseClient client, final int maxRetries)
	{
		super("MailStorageQueue-thread-" + threadNo);
		this.incomingQueue = new LinkedBlockingQueue<SerializedMail>();
		this.lookupMap = new IntObjectOpenHashMap<ReceiverLookupDocument>();
		this.receiverOnQueue = new IntOpenHashSet();
		this.threadNo = threadNo;
		this.mailStorageQueue = mailStorageQueue;
		this.client = client;
		this.maxRetries = maxRetries;
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
	
	@Override
	public void run()
	{
		while (running.get() || incomingQueue.size() > 0)
		{
			try
			{
				final SerializedMail mail = incomingQueue.take();
				processMail(mail);
			}
			catch (InterruptedException e)
			{
				System.err.println("Could not take mail from queue: " + e.getMessage());
			}
		}
		mailStorageQueue.countdown();
	}
	
	private void processMail(final SerializedMail mail)
	{
		final int receiverID = mail.getReceiverID();
		if (mail instanceof PoisionedSerializedMail)
		{
			// Do nothing, this mail just unblocks the queue so the thread can shutdown properly
			return;
		}
		else if (mail instanceof PersistLookupDocumentOnly)
		{
			// Persist lookup document
			if (receiverOnQueue.contains(receiverID))
			{
				if (!persistLookup(receiverID, createLookupDocumentKey(receiverID), lookupMap.get(receiverID)))
				{
					incomingQueue.add(mail);
				}
			}
		}
		else
		{
			// Persist mail
			final ReceiverLookupDocument receiverLookupDocument = getLookup(receiverID);
			if (receiverLookupDocument == null || !persistMail(mail))
			{
				incomingQueue.add(mail);
			}
			else
			{
				addMailToReceiverLookupDocument(mail, receiverLookupDocument);
			}
		}
	}
	
	private void addMailToReceiverLookupDocument(final SerializedMail mail, final ReceiverLookupDocument receiverLookupDocument)
	{
		final int receiverID = mail.getReceiverID();
		final String lookupDocumentKey = createLookupDocumentKey(receiverID);

		// Modify lookup document
		if (!receiverLookupDocument.add(mail.getCreated(), mail.getKey()))
		{
			System.err.println("#" + threadNo + " Could not add mail with same key for mail " + mail.getKey());
			return;
		}
		
		// Store lookup document
		if (!persistLookup(receiverID, lookupDocumentKey, receiverLookupDocument))
		{
			removeMailFromReceiverLookupDocument(mail, receiverLookupDocument);
		}
	}
	
	private void removeMailFromReceiverLookupDocument(final SerializedMail mail, final ReceiverLookupDocument receiverLookupDocument)
	{
		final int receiverID = mail.getReceiverID();
		final String lookupDocumentKey = createLookupDocumentKey(receiverID);
		
		receiverLookupDocument.remove(lookupDocumentKey);
		if (!receiverOnQueue.contains(receiverID))
		{
			receiverOnQueue.add(receiverID);
			incomingQueue.add(new PersistLookupDocumentOnly(receiverID));
		}
	}
	
	private ReceiverLookupDocument getLookup(final int receiverID)
	{
		final String lookupDocumentKey = createLookupDocumentKey(receiverID);
		ReceiverLookupDocument receiverLookupDocument = lookupMap.get(receiverID);
		if (receiverLookupDocument != null)
		{
			return receiverLookupDocument;
		}
		int tries = 0;
		while (tries++ < maxRetries)
		{
			try
			{
				final Object receiverLookupDocumentObject = client.get(lookupDocumentKey);
				if (receiverLookupDocumentObject != null)
				{
					receiverLookupDocument = ReceiverLookupDocument.fromJSON((String)receiverLookupDocumentObject);
				}
				else
				{
					receiverLookupDocument = new ReceiverLookupDocument();
				}
				lookupMap.put(receiverID, receiverLookupDocument);
				return receiverLookupDocument;
			}
			catch (Exception e)
			{
				System.err.println("#" + threadNo + " Could not read lookup document for " + receiverID + ": " + e.getMessage());
				threadSleep(tries * RETRY_DELAY);
			}
		}
		return null;
	}
	
	private boolean persistLookup(final int receiverID, final String lookupDocumentKey, final ReceiverLookupDocument receiverLookupDocument)
	{
		final String document = receiverLookupDocument.toJSON();
		int tries = 0;
		while (tries++ < maxRetries)
		{
			try
			{
				OperationFuture<Boolean> lookupFuture = client.set(lookupDocumentKey, 0, document);
				if (lookupFuture.get())
				{
					receiverOnQueue.remove(receiverID);
					printStatusMessage("#" + threadNo + " Lookup document for receiver " + receiverID + " successfully saved", receiverLookupDocument);
					return true;
				}
			}
			catch (Exception e)
			{
				receiverLookupDocument.setLastException(e);
				System.err.println("#" + threadNo + " Could not set lookup document for receiver " + receiverID + ": " + e.getMessage());
				threadSleep(tries * RETRY_DELAY);
			}
		}
		receiverLookupDocument.incrementTries();
		threadSleep(RETRY_DELAY);
		return false;
	}
	
	private boolean persistMail(final SerializedMail mail)
	{
		int tries = 0;
		while (tries++ < maxRetries)
		{
			try
			{
				OperationFuture<Boolean> mailFuture = client.set("mail_" + mail.getKey(), 0, mail.getDocument());
				if (mailFuture.get())
				{
					printStatusMessage("#" + threadNo + " Mail mail_" + mail.getKey() + " successfully saved", mail);
					return true;
				}
			}
			catch (Exception e)
			{
				mail.setLastException(e);
				System.err.println("#" + threadNo + " Could not add mail_" + mail.getKey() + " for receiver " + mail.getReceiverID() + ": " + e.getMessage());
				threadSleep(tries * RETRY_DELAY);
			}
		}
		mail.incrementTries();
		threadSleep(RETRY_DELAY);
		return false;
	}
	
	private String createLookupDocumentKey(final int receiverID)
	{
		return "receiver_" + receiverID;
	}
	
	private void printStatusMessage(String statusMessage, final StatusMessageObject statusMessageObject)
	{
		Boolean showMessage = false;
		if (statusMessageObject.getTries() > 0)
		{
			statusMessage += ", Tries: " + statusMessageObject.getTries();
			showMessage = true;
		}
		if (statusMessageObject.getLastException() != null)
		{
			statusMessage += ", Last Exception " + statusMessageObject.getLastException().getMessage();
			showMessage = true;
		}
		if (showMessage)
		{
			System.out.println(statusMessage);
		}
		statusMessageObject.resetStatus();
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
	
	private class PersistLookupDocumentOnly extends SerializedMail
	{
		public PersistLookupDocumentOnly(final int receiverID)
		{
			super(receiverID, 0, null, null);
		}
	}
}
