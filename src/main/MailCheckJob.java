package main;

import java.util.Map;

import net.spy.memcached.internal.OperationFuture;
import queue.ReceiverLookupDocument;

import com.couchbase.client.CouchbaseClient;

import database.ConnectionManager;

public class MailCheckJob implements Runnable
{
	@Override
	public void run()
	{
		final CouchbaseClient client = ConnectionManager.getConnection();
		
		System.out.println("Checking lookup documents and mails...");
		
		int numberOfMailsExpected = 0;
		int numberOfMailsFound = 0;
		int numberOfReceivers = 0;
		for (int receiverID = Config.MIN_USER; receiverID <= Config.MAX_USER; ++receiverID)
		{
			if (receiverID % 5000 == 0)
			{
				System.out.println("User " + receiverID + "/" + Config.MAX_USER);
			}
			String lookupFile = null;
			int getTries = 0;
			while (lookupFile == null && ++getTries < Config.MAX_RETRIES)
			{
				try
				{
					lookupFile = (String)client.get("receiver_" + receiverID);
				}
				catch (Exception e)
				{
					try
					{
						Thread.sleep(10);
					}
					catch (InterruptedException e1)
					{
					}
				}
			}
			if (lookupFile != null)
			{
				boolean sizeShown = false;
				ReceiverLookupDocument mailReceiverDocument = ReceiverLookupDocument.fromJSON(lookupFile);
				numberOfMailsExpected += mailReceiverDocument.size();
				numberOfReceivers++;
				for (String mailKey : mailReceiverDocument.keySet())
				{
					Map<String, String> mailStats = null;
					getTries = 0;
					while (mailStats == null && ++getTries < Config.MAX_RETRIES)
					{
						try
						{
							OperationFuture<Map<String, String>> future = client.getKeyStats("mail_" + mailKey);
							mailStats = future.get();
						}
						catch (Exception e)
						{
							try
							{
								Thread.sleep(10);
							}
							catch (InterruptedException e1)
							{
							}
						}
					}
					if (mailStats != null && mailStats.size() > 0)
					{
						numberOfMailsFound++;
					}
					else
					{
						if (!sizeShown)
						{
							System.out.println("Receiver " + receiverID + " should have " + mailReceiverDocument.size() + " mails");
							sizeShown = true;
						}
						System.err.println("Mail not found for receiver " + receiverID + ": mail_" + mailKey);
					}
				}
			}
			else
			{
				System.err.println("Lookup document not found for receiver " + receiverID);
			}
		}
		System.out.println("Number of receivers: " + numberOfReceivers);
		System.out.println("Number of mails expected: " + numberOfMailsExpected);
		System.out.println("Number of mails found: " + numberOfMailsFound);
		System.out.println("Number of documents expected: " + (numberOfMailsExpected + numberOfReceivers));
		System.out.println("Number of documents found: " + (numberOfMailsFound + numberOfReceivers));
		System.out.println("Mismatch: " + (numberOfMailsExpected - numberOfMailsFound));
	}
}
