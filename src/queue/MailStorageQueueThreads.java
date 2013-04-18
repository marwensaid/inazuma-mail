package queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;

class MailStorageQueueThreads extends Thread
{
	private final AtomicBoolean running = new AtomicBoolean(true);
	private final BlockingQueue<SerializedMail> queue;
	private final MailStorageQueue distributor;
	private final CouchbaseClient client;
	private final int maxRetries;
	
	protected MailStorageQueueThreads(final MailStorageQueue distributor, final CouchbaseClient client, final int maxRetries)
	{
		this.queue = new LinkedBlockingQueue<SerializedMail>();
		this.distributor = distributor;
		this.client = client;
		this.maxRetries = maxRetries;
	}

	@Override
	public void run()
	{
		while (running.get() || queue.size() > 0)
		{
			try
			{
				final SerializedMail mail = queue.take();
				if (mail != null && !(mail instanceof PoisionedSerializedMail))
				{
					if (mailReceiverLookupUpdate(mail))
					{
						persistMail(mail);
					}
				}
			}
			catch (InterruptedException e)
			{
			}
		}
		distributor.countdown();
	}
	
	protected void addMail(final SerializedMail mail)
	{
		queue.add(mail);
	}
	
	protected void shutdown()
	{
		running.set(false);
		queue.add(new PoisionedSerializedMail());
	}
	
	private boolean mailReceiverLookupUpdate(final SerializedMail mail)
	{
		// Lookup or create lookup document
		final String lookupDocumentKey = "receiver_" + mail.getReceiverID();
		int totalTries = 0;
		while (++totalTries < maxRetries)
		{
			CASValue<Object> casValue = null;
			int tries = 0;
			while (casValue == null && ++tries < maxRetries)
			{
				try
				{
					casValue = client.gets(lookupDocumentKey);
				}
				catch (Exception e)
				{
					try
					{
						Thread.sleep(10 + (tries * 5));
					}
					catch (InterruptedException e1)
					{
					}
				}
			}
			ReceiverLookupDocument mailReceiverLookup;
			if (casValue == null)
			{
				mailReceiverLookup = new ReceiverLookupDocument();
				//System.out.println("Add new lookup document for receiver " + mail.getReceiverID());
			}
			else
			{
				mailReceiverLookup = ReceiverLookupDocument.fromJSON((String)casValue.getValue());
				//System.out.println("Update existing lookup document for receiver " + mail.getReceiverID() + " (" + mailReceiverLookup.getLookup().toString() + ")");
			}
			
			// Modify lookup document
			if (!mailReceiverLookup.add(mail.getCreated(), mail.getKey()))
			{
				//System.err.println("Could not add mail with same key for mail " + mail.getKey());
				return false;
			}
			
			// Store lookup document
			if (casValue == null)
			{
				// Insert new lookup document
				try
				{
					OperationFuture<Boolean> addSuccess = client.add("receiver_" + mail.getReceiverID(), 0, mailReceiverLookup.toJSON());
					if (addSuccess.get())
					{
						// System.out.println("Saved new lookup document for mail_" + mail.getKey());
						return true;
					}
				}
				catch (Exception e)
				{
					//System.err.println("Could not add lookup document for mail_" + mail.getKey());
					return false;
				}
			}
			else
			{
				try
				{
					// Update existing lookup document
					CASResponse response = client.cas(lookupDocumentKey, casValue.getCas(), mailReceiverLookup.toJSON());
					if (response.equals(CASResponse.OK))
					{
						//System.out.println("Updated lookup document for mail_" + mail.getKey() + " (" + mailReceiverLookup.getLookup().toString() + ")");
						return true;
					}
				}
				catch (Exception e)
				{
					//System.err.println("Could not update lookup document for mail_" + mail.getKey());
					return false;
				}
			}
		}
		//System.err.println("Could not update lookup document within " + maxRetries + " tries for mail_" + mail.getKey());
		return false;
	}
	
	private void persistMail(final SerializedMail mail)
	{
		int tries = 0;
		while (++tries < maxRetries)
		{
			try
			{
				OperationFuture<Boolean> addSuccess = client.add("mail_" + mail.getKey(), 0, mail.getDocument());
				if (addSuccess.get())
				{
					return;
				}
			}
			catch (Exception e)
			{
				try
				{
					Thread.sleep(10 + (tries * 5));
				}
				catch (InterruptedException e1)
				{
					e1.printStackTrace();
				}
			}
		}
		System.err.println("Could not persist mail for receiver " + mail.getReceiverID() + ": mail_" + mail.getKey());
	}
	
	private class PoisionedSerializedMail extends SerializedMail
	{
		public PoisionedSerializedMail()
		{
			super(0, 0, null, null);
		}
	}
}
