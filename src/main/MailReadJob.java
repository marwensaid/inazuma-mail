package main;

import queue.ReceiverLookupDocument;

import com.couchbase.client.CouchbaseClient;

import database.ConnectionManager;

public class MailReadJob implements Runnable
{
	@Override
	public void run()
	{
		final CouchbaseClient client = ConnectionManager.getConnection();
		
		int numberOfMails = 0;
		int numberOfReceivers = 0;
		for (int i = Config.MIN_USER; i <= Config.MAX_USER; ++i)
		{
			String lookupFile = (String)client.get("receiver_" + i);
			if (lookupFile != null)
			{
				ReceiverLookupDocument mailReceiverDocument = ReceiverLookupDocument.fromJSON(lookupFile);
				System.out.println("Receiver " + i + ": " + mailReceiverDocument.getLookup().size());
				numberOfMails += mailReceiverDocument.getLookup().size();
				numberOfReceivers++;
			}
		}
		System.out.println("Number of receivers: " + numberOfReceivers);
		System.out.println("Number of referenced mails: " + numberOfMails);
		System.out.println("Number of documents: " + (numberOfMails + numberOfReceivers));
	}
}
