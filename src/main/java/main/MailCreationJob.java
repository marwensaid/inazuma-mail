package main;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import mail.Mail;
import mail.MailTrade;
import mail.MailUser;
import mail.MailType;
import mailstorage.MailStorage;
import mailstorage.SerializedMail;

import com.google.gson.Gson;

public class MailCreationJob implements Runnable
{
	private static final Gson gson = new Gson();
	private static final MailType[] mailTypes = MailType.values();
	private static final Random generator = new Random();

	private final ScheduledExecutorService threadPool;
	private final MailStorage mailStorageQueue;

	public MailCreationJob(final ScheduledExecutorService threadPool, final MailStorage mailStorageQueue)
	{
		this.threadPool = threadPool;
		this.mailStorageQueue = mailStorageQueue;
	}

	@Override
	public void run()
	{
		if (threadPool.isShutdown() || threadPool.isTerminated())
		{
			return;
		}

		// Create random mail document
		final int senderID = Config.MIN_USER + generator.nextInt(Config.MAX_USER);
		final int receiverID = Config.MIN_USER + generator.nextInt(Config.MAX_USER);
		final MailType mailType = mailTypes[1 + generator.nextInt(mailTypes.length - 1)];
		Mail mail = null;
		switch (mailType)
		{
			case USER:
			{
				final int textIndex = 1 + generator.nextInt(Config.SUBJECTS.length - 1);
				mail = new MailUser(senderID, receiverID, Config.SUBJECTS[textIndex], Config.BODIES[textIndex]);
				break;
			}
			case TRADE:
			{
				final ArrayList<Long> items = new ArrayList<Long>();
				final int itemCount = 1 + generator.nextInt(5);
				for (int i = 0; i < itemCount; ++i)
				{
					items.add(Math.abs(generator.nextLong()));
				}
				mail = new MailTrade(senderID, receiverID, items);
				break;
			}
		}

		// Put mail on storage queue
		mailStorageQueue.addMail(new SerializedMail(receiverID, mail.getCreated(), mail.getKey(), gson.toJson(mail)));

		if (!threadPool.isShutdown() && !threadPool.isTerminated())
		{
			threadPool.schedule(this, Config.CREATION_DELAY, Config.CREATION_TIMEUNIT);
		}
	}
}
